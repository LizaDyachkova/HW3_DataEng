"""Общие функции для ДЗ 3: подключение к S3, чтение объекта кусками, правило отбора вакансий.

Совместимо с Python 3.9.
"""
import csv
import re

import boto3
from botocore.config import Config

import config

csv.field_size_limit(2 ** 31 - 1)

# Правило отбора вакансий варианта «Программист, разработчик на C# и .NET»
# 1) role = «Программист, разработчик»
# 2) в названии вакансии (name) ИЛИ в ключевых навыках (key_skills) встречается
#    C#, C Sharp, .NET или dotnet (регистр не важен).
# Поле description НЕ используется: там слишком много случайных упоминаний.
ROLE = "Программист, разработчик"

# Тот же шаблон написан в skills.sql (DuckDB, движок RE2) — они должны совпадать.
VARIANT_PATTERN = r"c#|c ?sharp|\.net([^a-z]|$)|dotnet"
VARIANT_RE = re.compile(VARIANT_PATTERN)

# В данных встречается русская буква «с» вместо латинской «c» в «С#».
# Перед поиском и подсчётом заменяем «с#» (кириллица U+0441) на «c#» (латиница).
CYR_CSHARP = "с#"
LAT_CSHARP = "c#"

# Быстрая предпроверка по сырым байтам строки CSV (оптимизация). Это надмножество
# правила: любое совпадение с VARIANT_RE содержит один из этих кусков.
PREFILTER_TOKENS = (b"#", b".net", b"sharp", b"dotnet")


def norm_text(text):
    return text.lower().replace(CYR_CSHARP, LAT_CSHARP)


def norm_skill(text):
    return text.strip(" ").lower().replace(CYR_CSHARP, LAT_CSHARP)


def matches_variant(name, key_skills):
    return bool(VARIANT_RE.search(norm_text(name)) or VARIANT_RE.search(norm_text(key_skills)))


def parse_skills(key_skills):
    """Уникальные навыки вакансии (нормализованные), отсортированные по алфавиту."""
    seen = set()
    for part in key_skills.split(";"):
        skill = norm_skill(part)
        if skill:
            seen.add(skill)
    return sorted(seen)


def prefilter(line_bytes):
    low = line_bytes.lower()
    for token in PREFILTER_TOKENS:
        if token in low:
            return True
    return False


# S3
def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=config.ENDPOINT,
        aws_access_key_id=config.ACCESS_KEY,
        aws_secret_access_key=config.SECRET_KEY,
        region_name=config.REGION,
        config=Config(s3={"addressing_style": "path"},
                      retries={"max_attempts": 5, "mode": "standard"}),
    )


def object_size(s3, bucket, key):
    return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]


def get_range(s3, bucket, key, first, last):
    """Байты объекта от first до last включительно (HTTP Range)."""
    resp = s3.get_object(Bucket=bucket, Key=key, Range="bytes=%d-%d" % (first, last))
    return resp["Body"].read()


def iter_lines(s3, bucket, key, size, start, end, block):
    """Строки файла, НАЧИНАЮЩИЕСЯ в диапазоне [start, end).

    Объект читается по частям (HTTP Range) блоками по `block` байт.
    Правило границ: строка принадлежит куску, в котором лежит её первый байт.
      * если start > 0, начинаем читать с байта start-1: если там перевод строки,
        строка начинается ровно в start, иначе первую (неполную) строку пропускаем —
        её обработает предыдущий кусок;
      * последняя строка куска может выходить за end — дочитываем её следующими блоками.
    Возвращает пары (смещение_начала_строки, байты_строки_без_перевода_строки).
    """
    if start >= size:
        return
    pos = start - 1 if start > 0 else 0
    buf = b""
    buf_off = pos
    skipping = start > 0
    eof = False
    while True:
        if pos >= size:
            eof = True
        else:
            last = min(pos + block, size) - 1
            data = get_range(s3, bucket, key, pos, last)
            buf += data
            pos = last + 1
            eof = pos >= size

        if skipping:
            if not buf:
                if eof:
                    return
                continue
            if buf[:1] == b"\n":
                buf = buf[1:]
                buf_off += 1
                skipping = False
            else:
                j = buf.find(b"\n")
                if j < 0:
                    buf_off += len(buf)
                    buf = b""
                    if eof:
                        return
                    continue
                buf = buf[j + 1:]
                buf_off += j + 1
                skipping = False

        i0 = 0
        while True:
            j = buf.find(b"\n", i0)
            if j < 0:
                break
            off = buf_off + i0
            if off >= end:
                return
            line = buf[i0:j]
            if line.endswith(b"\r"):
                line = line[:-1]
            yield off, line
            i0 = j + 1
        buf = buf[i0:]
        buf_off += i0

        if buf_off >= end:
            return
        if eof:
            if buf:
                if buf.endswith(b"\r"):
                    buf = buf[:-1]
                yield buf_off, buf
            return


def read_header(s3, bucket, key, size, block=1 << 20):
    for _off, line in iter_lines(s3, bucket, key, size, 0, 1, block):
        return next(csv.reader([line.decode("utf-8-sig")]))
    raise RuntimeError("пустой файл")


def make_chunks(size, chunk_bytes):
    """Список (номер, начало, конец) — равные куски по chunk_bytes, последний короче."""
    chunks = []
    start = 0
    cid = 0
    while start < size:
        end = min(start + chunk_bytes, size)
        chunks.append((cid, start, end))
        cid += 1
        start = end
    return chunks

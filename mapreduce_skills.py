#!/usr/bin/env python3
"""Этап 3 (способ 1). Собственный MapReduce: частоты навыков, пар и троек навыков.

Данные читаются ИЗ ОБЪЕКТНОГО ХРАНИЛИЩА (RustFS, бакет hh-lake) частями по HTTP Range.
Результаты (для трёх размеров набора: 1, 2, 3 навыка) записываются обратно в бакет.

Схема работы:
  MAP     каждый маппер берёт свой диапазон байт объекта, находит строки, начинающиеся
          в этом диапазоне, отбирает вакансии варианта (C#/.NET), считает навыки/пары/тройки
          и локально их складывает (combiner);
  SHUFFLE маппер раскладывает ключи по R корзинам: crc32(ключ) % R, пишет временные файлы
          (запись атомарная: сначала .tmp, затем переименование — упавший маппер ничего
          «наполовину» не оставляет);
  REDUCE  редьюсер r читает корзину r от всех мапперов и суммирует счётчики;
  OUTPUT  итог сортируется и записывается в S3: results/<папка>/skills_1.csv, skills_2.csv,
          skills_3.csv и summary.json.

Отказ маппера: упавший кусок перезапускается (до MAX_ATTEMPTS раз). Для теста есть
--fail-chunk N (маппер N «падает» на первой попытке на середине куска).

Запуск (из папки проекта, в активированном виртуальном окружении):
    python mapreduce_skills.py                           # оптимизированная версия
    python mapreduce_skills.py --mode baseline           # исходная версия (для сравнения «до/после»)
    python mapreduce_skills.py --fail-chunk 5            # тест отказа маппера
"""
import argparse
import csv
import io
import json
import os
import shutil
import sys
import tempfile
import time
import zlib
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from concurrent.futures.process import BrokenProcessPool
from itertools import combinations
from math import comb
import pickle

import common
import config

MAX_ATTEMPTS = 4


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def partition(key, n_parts):
    # Python hash() для строк случаен в каждом процессе, поэтому берём crc32.
    return zlib.crc32("\x1f".join(key).encode("utf-8")) % n_parts


# MAP
def map_task(task):
    cid = task["cid"]
    start, end = task["start"], task["end"]
    attempt = task["attempt"]
    t0 = time.time()

    s3 = common.make_s3()
    cols = task["header"]
    idx = {name: i for i, name in enumerate(cols)}
    i_role, i_name, i_skills = idx["role"], idx["name"], idx["key_skills"]
    ncols = len(cols)

    use_prefilter = task["mode"] == "optimized"
    fail_here = (task["fail_chunk"] == cid and attempt == 0)
    fail_at = start + (end - start) // 2

    singles, pairs, triples = Counter(), Counter(), Counter()
    rows = matched = bad = 0
    emit1 = emit2 = emit3 = 0
    expected2 = expected3 = 0
    skills_per_vac = Counter()

    for off, line in common.iter_lines(s3, config.BUCKET, config.CSV_KEY, task["size"],
                                       start, end, task["block"]):
        if off == 0:
            continue
        if fail_here and off >= fail_at:
            if task["crash"] == "kill":
                os._exit(1)
            raise RuntimeError("имитация отказа маппера %d (попытка %d) на смещении %d" % (cid, attempt, off))
        rows += 1
        if use_prefilter and not common.prefilter(line):
            continue
        row = next(csv.reader([line.decode("utf-8")]))
        if len(row) != ncols:
            bad += 1
            continue
        if row[i_role] != common.ROLE:
            continue
        if not common.matches_variant(row[i_name], row[i_skills]):
            continue
        matched += 1
        skills = common.parse_skills(row[i_skills])
        n = len(skills)
        skills_per_vac[n] += 1
        for s in skills:
            singles[(s,)] += 1
        emit1 += n
        for c in combinations(skills, 2):
            pairs[c] += 1
        for c in combinations(skills, 3):
            triples[c] += 1
        expected2 += comb(n, 2)
        expected3 += comb(n, 3)
        emit2 += comb(n, 2)
        emit3 += comb(n, 3)

    # SHUFFLE: раскладываем по корзинам и пишем временные файлы
    R = task["reducers"]
    buckets = [({}, {}, {}) for _ in range(R)]
    for k, v in singles.items():
        buckets[partition(k, R)][0][k] = v
    for k, v in pairs.items():
        buckets[partition(k, R)][1][k] = v
    for k, v in triples.items():
        buckets[partition(k, R)][2][k] = v
    tmp_names = []
    for r in range(R):
        final = os.path.join(task["workdir"], "m%04d.r%d.pkl" % (cid, r))
        tmp = final + ".a%d.tmp" % attempt
        with open(tmp, "wb") as f:
            pickle.dump(buckets[r], f, protocol=pickle.HIGHEST_PROTOCOL)
        tmp_names.append((tmp, final))
    for tmp, final in tmp_names:
        os.replace(tmp, final)

    return {
        "cid": cid, "attempt": attempt, "rows": rows, "matched": matched, "bad": bad,
        "emit": [emit1, emit2, emit3],
        "combined": [len(singles), len(pairs), len(triples)],
        "expected": [emit1, expected2, expected3],
        "skills_per_vac": dict(skills_per_vac),
        "sec": time.time() - t0,
    }


# REDUCE
def reduce_task(task):
    r = task["r"]
    t0 = time.time()
    totals = (Counter(), Counter(), Counter())
    for cid in task["cids"]:
        path = os.path.join(task["workdir"], "m%04d.r%d.pkl" % (cid, r))
        with open(path, "rb") as f:
            parts = pickle.load(f)
        for total, part in zip(totals, parts):
            total.update(part)
    return {"r": r, "result": [dict(t) for t in totals], "sec": time.time() - t0}


# Запуск стадии с повторами
def run_stage(name, fn, tasks, workers, key="cid"):
    """Выполняет задачи в пуле процессов. Упавшую задачу перезапускает (до MAX_ATTEMPTS).

    Если упал сам процесс (BrokenProcessPool), пул пересоздаётся, незавершённые
    задачи отправляются заново."""
    results = {}
    attempts = defaultdict(int)
    todo = {t[key]: t for t in tasks}
    while todo:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            pending = {}
            for k, t in list(todo.items()):
                t = dict(t, attempt=attempts[k])
                pending[ex.submit(fn, t)] = k
            broken = False
            while pending and not broken:
                done, _ = wait(list(pending), return_when=FIRST_COMPLETED)
                for fut in done:
                    k = pending.pop(fut)
                    try:
                        results[k] = fut.result()
                        todo.pop(k, None)
                    except BrokenProcessPool:
                        broken = True
                    except Exception as e:
                        attempts[k] += 1
                        log("%s %s=%s: ОШИБКА (попытка %d): %s" % (name, key, k, attempts[k], e))
                        if attempts[k] >= MAX_ATTEMPTS:
                            raise
                        log("%s %s=%s: перезапускаю" % (name, key, k))
                        # пул продолжает работать, повтор запустим в следующем круге
            if broken:
                log("%s: процесс-исполнитель аварийно завершился, пересоздаю пул и перезапускаю "
                    "незавершённые задачи" % name)
                for k in todo:
                    attempts[k] += 1
                    if attempts[k] >= MAX_ATTEMPTS:
                        raise RuntimeError("слишком много сбоев: %s %s" % (name, k))
    return results


# Вывод результатов в S3
def rows_to_csv(header, items):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    for key, cnt in items:
        w.writerow(list(key) + [cnt])
    return buf.getvalue().encode("utf-8")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["baseline", "optimized"], default="optimized",
                    help="baseline: разбирает все строки CSV, читает кусок целиком; "
                         "optimized: предпроверка по байтам + потоковое чтение блоками")
    ap.add_argument("--chunk-mb", type=int, default=64, help="размер куска для одного маппера, МБ")
    ap.add_argument("--block-mb", type=int, default=None,
                    help="размер одного HTTP Range-запроса, МБ (по умолчанию: baseline = размеру куска, optimized = 8)")
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1))
    ap.add_argument("--reducers", type=int, default=None, help="число редьюсеров (по умолчанию = workers)")
    ap.add_argument("--out", default="mapreduce", help="папка результатов внутри results/ в бакете")
    ap.add_argument("--fail-chunk", type=int, default=-1, help="номер куска, где маппер упадёт на 1-й попытке")
    ap.add_argument("--crash", choices=["exception", "kill"], default="exception",
                    help="как «ломать» маппер: исключение или убийство процесса")
    args = ap.parse_args()

    chunk_bytes = args.chunk_mb * 1024 * 1024
    block_mb = args.block_mb if args.block_mb else (args.chunk_mb if args.mode == "baseline" else 8)
    block = block_mb * 1024 * 1024
    R = args.reducers or args.workers

    t_start = time.time()
    s3 = common.make_s3()
    size = common.object_size(s3, config.BUCKET, config.CSV_KEY)
    header = common.read_header(s3, config.BUCKET, config.CSV_KEY, size)
    chunks = common.make_chunks(size, chunk_bytes)
    log("объект s3://%s/%s: %d байт (%.1f МБ), колонок %d" % (config.BUCKET, config.CSV_KEY, size, size / 1048576, len(header)))
    log("режим=%s, кусков=%d по %d МБ, блок чтения=%d МБ, мапперов параллельно=%d, редьюсеров=%d"
        % (args.mode, len(chunks), args.chunk_mb, block_mb, args.workers, R))
    if args.fail_chunk >= 0:
        log("ТЕСТ ОТКАЗА: маппер куска %d упадёт на первой попытке (%s)" % (args.fail_chunk, args.crash))

    workdir = tempfile.mkdtemp(prefix="mr_shuffle_")
    try:
        t0 = time.time()
        map_tasks = [dict(cid=c, start=s, end=e, size=size, header=header, mode=args.mode, block=block,
                          reducers=R, workdir=workdir, fail_chunk=args.fail_chunk, crash=args.crash)
                     for c, s, e in chunks]
        map_res = run_stage("MAP", map_task, map_tasks, args.workers, key="cid")
        t_map = time.time() - t0
        for cid in sorted(map_res):
            m = map_res[cid]
            log("  маппер %2d: строк %6d, вакансий варианта %5d, попытка %d, %.1f с"
                % (cid, m["rows"], m["matched"], m["attempt"] + 1, m["sec"]))
        log("MAP завершён за %.1f с" % t_map)

        t0 = time.time()
        red_tasks = [dict(r=r, cids=sorted(map_res), workdir=workdir) for r in range(R)]
        red_res = run_stage("REDUCE", reduce_task, red_tasks, min(args.workers, R), key="r")
        t_reduce = time.time() - t0
        log("REDUCE завершён за %.1f с" % t_reduce)

        t0 = time.time()
        finals = [{}, {}, {}]
        for r in range(R):
            for level in range(3):
                finals[level].update(red_res[r]["result"][level])
        headers = [["skill", "count"], ["skill_1", "skill_2", "count"], ["skill_1", "skill_2", "skill_3", "count"]]
        names = ["skills_1.csv", "skills_2.csv", "skills_3.csv"]
        for level in range(3):
            items = sorted(finals[level].items(), key=lambda kv: (-kv[1], kv[0]))
            body = rows_to_csv(headers[level], items)
            key = "results/%s/%s" % (args.out, names[level])
            s3.put_object(Bucket=config.BUCKET, Key=key, Body=body)
            log("записано s3://%s/%s: %d строк, %d байт" % (config.BUCKET, key, len(items), len(body)))

        total_rows = sum(m["rows"] for m in map_res.values())
        matched = sum(m["matched"] for m in map_res.values())
        bad = sum(m["bad"] for m in map_res.values())
        emit = [sum(m["emit"][i] for m in map_res.values()) for i in range(3)]
        combined_local = [sum(m["combined"][i] for m in map_res.values()) for i in range(3)]
        expected = [sum(m["expected"][i] for m in map_res.values()) for i in range(3)]
        sums = [sum(finals[i].values()) for i in range(3)]
        distinct = [len(finals[i]) for i in range(3)]
        spv = Counter()
        for m in map_res.values():
            for n, v in m["skills_per_vac"].items():
                spv[int(n)] += v
        retries = sum(1 for m in map_res.values() if m["attempt"] > 0)
        t_total = time.time() - t_start
        summary = {
            "variant": "Программист, разработчик на C# и .NET",
            "rule": {"role": common.ROLE, "pattern": common.VARIANT_PATTERN,
                     "fields": ["name", "key_skills"], "normalization": "lower(); 'с#'(кириллица) -> 'c#'"},
            "object_size_bytes": size, "rows_scanned": total_rows, "rows_bad": bad,
            "vacancies_matched": matched,
            "distinct": {"skills": distinct[0], "pairs": distinct[1], "triples": distinct[2]},
            "occurrences": {"skills": sums[0], "pairs": sums[1], "triples": sums[2]},
            "combinatorics_check": {"sum_n": expected[0], "sum_C(n,2)": expected[1], "sum_C(n,3)": expected[2]},
            "shuffle": {"emitted_records": emit, "after_combiner": combined_local},
            "skills_per_vacancy": {str(k): spv[k] for k in sorted(spv)},
            "run": {"mode": args.mode, "chunk_mb": args.chunk_mb, "block_mb": block_mb, "workers": args.workers,
                    "reducers": R, "chunks": len(chunks), "mappers_retried": retries,
                    "fail_chunk": args.fail_chunk,
                    "sec_map": round(t_map, 2), "sec_reduce": round(t_reduce, 2), "sec_total": round(t_total, 2)},
        }
        s3.put_object(Bucket=config.BUCKET, Key="results/%s/summary.json" % args.out,
                      Body=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"))
        t_out = time.time() - t0

        print()
        print("=" * 64)
        print("ИТОГ MapReduce (режим %s)" % args.mode)
        print("=" * 64)
        print("прочитано строк данных:        %d" % total_rows)
        print("вакансий варианта C#/.NET:     %d" % matched)
        print("{:<26}{:>12}{:>18}".format("размер набора", "уникальных", "всего вхождений"))
        for label, i in (("1 навык (одиночные)", 0), ("2 навыка (пары)", 1), ("3 навыка (тройки)", 2)):
            print("{:<26}{:>12}{:>18}".format(label, distinct[i], sums[i]))
        ok = sums == expected
        print("проверка по числу сочетаний: Σn=%d, ΣC(n,2)=%d, ΣC(n,3)=%d -> %s"
              % (expected[0], expected[1], expected[2], "СОВПАЛО" if ok else "НЕ СОВПАЛО!"))
        print("записей на shuffle без combiner: %s, после combiner: %s" % (emit, combined_local))
        if args.fail_chunk >= 0:
            print("перезапущено мапперов после отказа: %d" % retries)
        print("время: map %.1f с, reduce %.1f с, запись %.1f с, всего %.1f с" % (t_map, t_reduce, t_out, t_total))
        print("=" * 64)
        if not ok:
            sys.exit(2)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Этап 4. Сверка результатов MapReduce и DuckDB (оба набора читаются из бакета).

    python verify_results.py                       # results/mapreduce  против  results/duckdb
    python verify_results.py results/mr_x results/duckdb
"""
import csv
import io
import json
import sys

import common
import config

FILES = [("skills_1.csv", 1), ("skills_2.csv", 2), ("skills_3.csv", 3)]


def load(s3, prefix, name, n_key):
    body = s3.get_object(Bucket=config.BUCKET, Key="%s/%s" % (prefix, name))["Body"].read().decode("utf-8")
    rows = list(csv.reader(io.StringIO(body)))
    header, data = rows[0], rows[1:]
    result = {}
    for r in data:
        key = tuple(r[:n_key])
        if key in result:
            raise RuntimeError("дубликат ключа в %s/%s: %s" % (prefix, name, key))
        result[key] = int(r[n_key])
    return header, result, body


def main():
    a_prefix = sys.argv[1] if len(sys.argv) > 1 else "results/mapreduce"
    b_prefix = sys.argv[2] if len(sys.argv) > 2 else "results/duckdb"
    s3 = common.make_s3()
    print("Сравниваю s3://%s/%s (MapReduce)  и  s3://%s/%s (DuckDB)" % (config.BUCKET, a_prefix, config.BUCKET, b_prefix))
    print("-" * 78)
    print("{:<14}{:>10}{:>14}{:>14}{:>14}  {}".format("файл", "строк MR", "строк DuckDB", "Σ MR", "Σ DuckDB", "результат"))
    all_ok = True
    for name, n in FILES:
        ha, a, body_a = load(s3, a_prefix, name, n)
        hb, b, body_b = load(s3, b_prefix, name, n)
        same = (a == b) and ha == hb
        all_ok &= same
        print("{:<14}{:>10}{:>14}{:>14}{:>14}  {}".format(name, len(a), len(b), sum(a.values()), sum(b.values()),
                                                          "СОВПАЛО" if same else "РАЗЛИЧИЯ"))
        if not same:
            only_a = [k for k in a if k not in b][:5]
            only_b = [k for k in b if k not in a][:5]
            diff = [(k, a[k], b[k]) for k in a if k in b and a[k] != b[k]][:5]
            print("   только в MR:", only_a)
            print("   только в DuckDB:", only_b)
            print("   разные счётчики (ключ, MR, DuckDB):", diff)
        else:
            print("   тексты файлов побайтно идентичны: %s" % ("да" if body_a == body_b else "нет (порядок/кавычки, значения те же)"))
    sa = json.loads(s3.get_object(Bucket=config.BUCKET, Key=a_prefix + "/summary.json")["Body"].read())
    sb = json.loads(s3.get_object(Bucket=config.BUCKET, Key=b_prefix + "/summary.json")["Body"].read())
    same_vac = sa["vacancies_matched"] == sb["vacancies_matched"]
    all_ok &= same_vac
    print("-" * 78)
    print("вакансий варианта: MapReduce %d, DuckDB %d -> %s" % (sa["vacancies_matched"], sb["vacancies_matched"],
                                                                "СОВПАЛО" if same_vac else "РАЗЛИЧИЯ"))
    print("ИТОГ: " + ("ВСЕ РЕЗУЛЬТАТЫ ПОЛНОСТЬЮ СОВПАЛИ" if all_ok else "ЕСТЬ РАСХОЖДЕНИЯ"))
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()

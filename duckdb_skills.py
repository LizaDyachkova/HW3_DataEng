#!/usr/bin/env python3
"""Этап 3 (способ 2). Запускает skills.sql в DuckDB и печатает контрольные числа.

    python duckdb_skills.py
"""
import json
import time

import duckdb

import common
import config


def main():
    sql = open("skills.sql", encoding="utf-8").read()
    con = duckdb.connect()
    t0 = time.time()
    for stmt in con.extract_statements(sql):
        con.execute(stmt)
    t_sql = time.time() - t0

    vac = con.execute("SELECT count(*) FROM vac").fetchone()[0]
    n_skills = con.execute("SELECT count(DISTINCT skill) FROM skills").fetchone()[0]
    total_1 = con.execute("SELECT count(*) FROM skills").fetchone()[0]
    per_vac = con.execute("SELECT skill_n, count(*) FROM (SELECT vacancy_id, count(*) AS skill_n FROM skills GROUP BY vacancy_id) GROUP BY skill_n").fetchall()
    total_2 = sum(n * (n - 1) // 2 * c for n, c in per_vac)
    total_3 = sum(n * (n - 1) * (n - 2) // 6 * c for n, c in per_vac)

    s3 = common.make_s3()
    sizes = {}
    for name in ("skills_1.csv", "skills_2.csv", "skills_3.csv"):
        sizes[name] = common.object_size(s3, config.BUCKET, "results/duckdb/" + name)

    summary = {"vacancies_matched": vac, "distinct_skills": n_skills,
               "occurrences": {"skills": total_1, "pairs": total_2, "triples": total_3},
               "sec_sql": round(t_sql, 2)}
    s3.put_object(Bucket=config.BUCKET, Key="results/duckdb/summary.json",
                  Body=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"))

    print("=" * 64)
    print("ИТОГ DuckDB")
    print("=" * 64)
    print("вакансий варианта C#/.NET:     %d" % vac)
    print("уникальных навыков:            %d" % n_skills)
    print("всего вхождений: навыки %d, пары %d, тройки %d" % (total_1, total_2, total_3))
    for k, v in sizes.items():
        print("в бакете: results/duckdb/%s, %d байт" % (k, v))
    print("время SQL: %.1f с" % t_sql)
    print("=" * 64)


if __name__ == "__main__":
    main()

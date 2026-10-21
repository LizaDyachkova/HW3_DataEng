#!/usr/bin/env python3
"""Этап 6. Региональный анализ: три региона, две таблицы + инсайты (DuckDB + данные из S3).

    python regions_analysis.py
    python regions_analysis.py --regions "Москва,Санкт-Петербург,Новосибирск" --top 10
"""
import argparse
import time

import duckdb


def show(title, rows, cols):
    print()
    print(title)
    print("-" * 110)
    print(" | ".join(cols))
    for r in rows:
        print(" | ".join("" if x is None else str(x) for x in r))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--regions", default="Москва,Санкт-Петербург,Новосибирск")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--min-lift", type=int, default=20, help="минимум вакансий для расчёта lift")
    ap.add_argument("--min-salary", type=int, default=20000)
    ap.add_argument("--max-salary", type=int, default=1000000)
    ap.add_argument("--sql", default="regions.sql")
    args = ap.parse_args()

    regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    literal = ", ".join("'%s'" % r.replace("'", "''") for r in regions)
    sql = (open(args.sql, encoding="utf-8").read()
           .replace("@REGIONS@", literal).replace("@TOP@", str(args.top)).replace("@MIN_LIFT@", str(args.min_lift))
           .replace("@MIN_SALARY@", str(args.min_salary)).replace("@MAX_SALARY@", str(args.max_salary)))

    con = duckdb.connect()
    t0 = time.time()
    for st in con.extract_statements(sql):
        con.execute(st)
    q = lambda text: con.execute(text).fetchall()

    show("ТАБЛИЦА 1. Ключевые показатели регионов (зарплаты «на руки», руб./мес., после чистки как в этапе 5)",
         q("SELECT * FROM reg_summary ORDER BY CASE WHEN area = 'Вся выборка' THEN 1 ELSE 0 END, vacancies DESC"),
         ["регион", "вакансий", "с навыками", "навыков в среднем", "с зарплатой", "медиана", "Q1", "Q3", "% без опыта", "% опыт >6 лет"])

    names = {1: "навыки", 2: "пары навыков", 3: "тройки навыков"}
    for region in regions:
        for size in (1, 2, 3):
            show("ТАБЛИЦА 2. %s — самые востребованные %s" % (region, names[size]),
                 q("SELECT rk, combo, vacancies, pct, with_salary, median_salary FROM reg_top "
                   "WHERE area = '%s' AND size = %d ORDER BY rk" % (region.replace("'", "''"), size)),
                 ["#", "комбинация", "вакансий", "% вакансий региона", "из них с зарплатой", "медиана зарплаты"])

    for region in regions:
        for size in (1, 2):
            show("ИНСАЙТ. %s — что встречается здесь чаще, чем в остальной выборке (%s)" % (region, names[size]),
                 q("SELECT rk, combo, in_region, elsewhere, lift FROM reg_lift "
                   "WHERE area = '%s' AND size = %d ORDER BY rk" % (region.replace("'", "''"), size)),
                 ["#", "комбинация", "вакансий в регионе", "вакансий в остальных", "lift"])
    print("\nфайлы: s3://hh-lake/results/regions/regions_summary.csv, regions_top_skills.csv, regions_lift.csv")
    print("время: %.1f с" % (time.time() - t0))


if __name__ == "__main__":
    main()

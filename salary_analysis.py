#!/usr/bin/env python3
"""Этап 5. Влияние пар и троек навыков на медианную зарплату (DuckDB + данные из S3).

    python salary_analysis.py
    python salary_analysis.py --min-pair 40 --min-triple 25
"""
import argparse
import time

import duckdb

import common
import config


def show(title, rows, cols):
    print()
    print(title)
    print("-" * 110)
    print(" | ".join(cols))
    for r in rows:
        print(" | ".join(str(x) for x in r))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-pair", type=int, default=30, help="минимум вакансий с зарплатой у пары")
    ap.add_argument("--min-triple", type=int, default=20, help="минимум вакансий с зарплатой у тройки")
    ap.add_argument("--min-employers", type=int, default=5, help="минимум разных работодателей у комбинации")
    ap.add_argument("--min-salary", type=int, default=20000, help="отбрасываем зарплаты ниже (руб./мес.)")
    ap.add_argument("--max-salary", type=int, default=1000000, help="отбрасываем зарплаты выше (руб./мес.)")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--sql", default="salary.sql")
    args = ap.parse_args()

    sql = open(args.sql, encoding="utf-8").read()
    sql = (sql.replace("@MIN_SALARY@", str(args.min_salary)).replace("@MAX_SALARY@", str(args.max_salary))
              .replace("@MIN_PAIR@", str(args.min_pair)).replace("@MIN_TRIPLE@", str(args.min_triple))
              .replace("@MIN_EMPLOYERS@", str(args.min_employers)))
    part1, part2 = sql.split("-- @@STAGE2@@")

    con = duckdb.connect()
    t0 = time.time()
    for st in con.extract_statements(part1):
        con.execute(st)

    q = lambda text: con.execute(text).fetchall()
    n_all = q("SELECT count(*) FROM vac_all")[0][0]
    n_rur = q("SELECT count(*) FROM sal_raw")[0][0]
    n_ok = q("SELECT count(*) FROM sal_clip")[0][0]
    n_dedup = q("SELECT count(*) FROM sal")[0][0]
    cur = q("SELECT coalesce(currency,'(нет)'), count(*) FROM vac_all WHERE sf IS NOT NULL OR st IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")
    kinds = q("SELECT count(*) FILTER (WHERE sf IS NOT NULL AND st IS NOT NULL), count(*) FILTER (WHERE sf IS NOT NULL AND st IS NULL), "
              "count(*) FILTER (WHERE sf IS NULL AND st IS NOT NULL) FROM vac_all WHERE currency='RUR'")[0]
    gross = q("SELECT coalesce(gross,'(не указано)'), count(*) FROM sal GROUP BY 1 ORDER BY 2 DESC")
    qs = q("SELECT round(min(net)), round(quantile_cont(net,0.25)), round(median(net)), round(quantile_cont(net,0.75)), round(max(net)) FROM sal")[0]

    print("=" * 78)
    print("ВЫБОРКА ДЛЯ АНАЛИЗА ЗАРПЛАТ")
    print("=" * 78)
    print("вакансий варианта C#/.NET:                        %d" % n_all)
    print("из них есть зарплата (хотя бы одно число), валюты: %s" % ", ".join("%s: %d" % c for c in cur))
    print("зарплата в рублях:                                 %d (вилка: %d, только «от»: %d, только «до»: %d)" % ((n_rur,) + tuple(kinds)))
    print("после отсева выбросов [%d; %d]:             %d (отброшено %d)" % (args.min_salary, args.max_salary, n_ok, n_rur - n_ok))
    print("после удаления повторов (работодатель + название + зарплата): %d (убрано %d)" % (n_dedup, n_ok - n_dedup))
    print("gross (до налога -> умножено на 0.87) в выборке:  %s" % ", ".join("%s: %d" % g for g in gross))
    print("зарплата «на руки», руб./мес.: min %s, Q1 %s, МЕДИАНА %s, Q3 %s, max %s" % qs)

    exp = q("SELECT coalesce(experience,'(нет)'), count(*), round(median(net)) FROM sal GROUP BY 1 ORDER BY 3")
    show("Зарплата по опыту (в выборке)", exp, ["опыт", "вакансий", "медиана"])
    junior = [e for e in exp if e[1] >= 30][0][0]
    print("\n«Самая низкооплачиваемая» категория опыта (для колонки pct_junior): %s" % junior)

    sql2 = part2.replace("@JUNIOR@", junior.replace("'", "''"))
    for st in con.extract_statements(sql2):
        con.execute(st)

    cols_p = ["base_skill", "extra_skill", "vacancies", "employers", "median_combo", "median_base", "vacancies_base", "gain", "gain_pct", "pct_moscow", "pct_junior"]
    cols_t = ["base_skill", "extra_skills"] + cols_p[2:]
    n_pairs = q("SELECT count(*) FROM pair_gain")[0][0]
    n_triples = q("SELECT count(*) FROM triple_gain")[0][0]
    print("\nпар с >= %d вакансий с зарплатой и >= %d работодателей: %d; троек с >= %d вакансий: %d"
          % (args.min_pair, args.min_employers, n_pairs, args.min_triple, n_triples))

    sel = ", ".join(cols_p)
    selt = ", ".join(cols_t)
    show("ПАРЫ: сильнее всего ПОВЫШАЮТ медиану (прибавка к медиане базового, более частого навыка)",
         q("SELECT %s FROM pair_gain ORDER BY gain DESC LIMIT %d" % (sel, args.top)), cols_p)
    show("ПАРЫ: сильнее всего ПОНИЖАЮТ медиану",
         q("SELECT %s FROM pair_gain ORDER BY gain ASC LIMIT %d" % (sel, args.top)), cols_p)
    show("ТРОЙКИ: сильнее всего ПОВЫШАЮТ медиану",
         q("SELECT %s FROM triple_gain ORDER BY gain DESC LIMIT %d" % (selt, args.top)), cols_t)
    show("ТРОЙКИ: сильнее всего ПОНИЖАЮТ медиану",
         q("SELECT %s FROM triple_gain ORDER BY gain ASC LIMIT %d" % (selt, args.top)), cols_t)

    show("Для сравнения: одиночные навыки с наибольшей медианой (>= %d вакансий)" % args.min_pair,
         q("SELECT skill, n, round(med) FROM skill_med WHERE n >= %d ORDER BY med DESC LIMIT %d" % (args.min_pair, args.top)),
         ["навык", "вакансий", "медиана"])
    show("Для сравнения: одиночные навыки с наименьшей медианой (>= %d вакансий)" % args.min_pair,
         q("SELECT skill, n, round(med) FROM skill_med WHERE n >= %d ORDER BY med ASC LIMIT %d" % (args.min_pair, args.top)),
         ["навык", "вакансий", "медиана"])
    print("\nфайлы с полными таблицами: s3://%s/results/salary/pairs_gain.csv и triples_gain.csv" % config.BUCKET)
    print("время: %.1f с" % (time.time() - t0))


if __name__ == "__main__":
    main()

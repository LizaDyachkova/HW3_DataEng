#!/usr/bin/env python3
"""Печатает summary.json результата MapReduce и таблицу роста числа пар и троек.

    python show_summary.py                 # results/mapreduce
    python show_summary.py results/mr_x
"""
import json
import sys
from math import comb

import common
import config


def main():
    prefix = sys.argv[1] if len(sys.argv) > 1 else "results/mapreduce"
    s3 = common.make_s3()
    sm = json.loads(s3.get_object(Bucket=config.BUCKET, Key=prefix + "/summary.json")["Body"].read())
    print("Сводка s3://%s/%s/summary.json" % (config.BUCKET, prefix))
    print("-" * 70)
    print("вариант: %s" % sm["variant"])
    print("правило: role = %r; в name или key_skills: %s" % (sm["rule"]["role"], sm["rule"]["pattern"]))
    print("строк в файле: %d, вакансий варианта: %d" % (sm["rows_scanned"], sm["vacancies_matched"]))
    d, o = sm["distinct"], sm["occurrences"]
    print()
    print("{:<22}{:>14}{:>18}{:>16}".format("размер набора", "уникальных", "всего вхождений", "рост (раз)"))
    print("{:<22}{:>14}{:>18}{:>16}".format("1 навык", d["skills"], o["skills"], "-"))
    print("{:<22}{:>14}{:>18}{:>16.1f}".format("2 навыка (пары)", d["pairs"], o["pairs"], o["pairs"] / o["skills"]))
    print("{:<22}{:>14}{:>18}{:>16.1f}".format("3 навыка (тройки)", d["triples"], o["triples"], o["triples"] / o["pairs"]))
    print()
    print("Откуда рост: вакансия с n навыками даёт n навыков, C(n,2) пар и C(n,3) троек.")
    print("{:>4}{:>11}{:>10}{:>10}{:>14}{:>14}".format("n", "вакансий", "C(n,2)", "C(n,3)", "пар всего", "троек всего"))
    tot_p = tot_t = 0
    for n_str, cnt in sorted(sm["skills_per_vacancy"].items(), key=lambda kv: int(kv[0])):
        n = int(n_str)
        tot_p += comb(n, 2) * cnt
        tot_t += comb(n, 3) * cnt
        print("{:>4}{:>11}{:>10}{:>10}{:>14}{:>14}".format(n, cnt, comb(n, 2), comb(n, 3), comb(n, 2) * cnt, comb(n, 3) * cnt))
    print("{:>4}{:>11}{:>10}{:>10}{:>14}{:>14}".format("итог", sm["vacancies_matched"], "", "", tot_p, tot_t))
    sh = sm["shuffle"]
    print()
    print("Записей на shuffle без combiner: %s, после combiner (внутри мапперов): %s" % (sh["emitted_records"], sh["after_combiner"]))


if __name__ == "__main__":
    main()

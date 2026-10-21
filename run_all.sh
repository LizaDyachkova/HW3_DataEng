#!/bin/bash
# Итоговый прогон ДЗ 3. Каждый шаг печатает вывод и сохраняет его в logs/<шаг>.txt
#   ./run_all.sh main      шаги 1-6: MapReduce, DuckDB, сверка, тест отказа маппера, сводка
#   ./run_all.sh analysis  шаги 7-8: зарплаты и регионы
#   ./run_all.sh profile   шаги 9-11: профилирование (по 3 запуска)
# Перед запуском: сервер RustFS работает, активировано виртуальное окружение (source .venv/bin/activate).
cd "$(dirname "$0")" || exit 1
mkdir -p logs profiling
export PYTHONWARNINGS=ignore

run() {
    name="$1"; shift
    echo
    echo "########## $name ##########"
    "$@" 2>&1 | tee "logs/$name.txt"
}

case "${1:-}" in
  main)
    run 01_mapreduce python mapreduce_skills.py
    run 02_duckdb python duckdb_skills.py
    run 03_verify python verify_results.py
    run 04_mapper_failure python mapreduce_skills.py --fail-chunk 5 --out mr_fail
    run 05_verify_after_failure python verify_results.py results/mr_fail results/duckdb
    run 06_summary python show_summary.py
    ;;
  analysis)
    run 07_salary python salary_analysis.py
    run 08_regions python regions_analysis.py
    ;;
  profile)
    rm -f profiling/results.csv
    run 09_profile_mr_baseline python profile_run.py --label mr-baseline --runs 3 -- python mapreduce_skills.py --mode baseline --out mr_baseline
    run 10_profile_mr_optimized python profile_run.py --label mr-optimized --runs 3 -- python mapreduce_skills.py --mode optimized --out mr_optimized
    run 11_profile_duckdb python profile_run.py --label duckdb --runs 3 -- python duckdb_skills.py
    ;;
  *)
    echo "Использование: ./run_all.sh main | analysis | profile"
    exit 1
    ;;
esac

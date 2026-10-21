# ДЗ 3. Технология MapReduce (вариант 4: «Программист, разработчик на C# и .NET»)

Дисциплина «Инженерия данных», ИТМО. Дьячкова Елизавета Александровна, группа J3212.

Собственная реализация MapReduce на Python: данные читаются из объектного хранилища RustFS (S3) кусками, результаты записываются обратно в бакет. Те же подсчёты сделаны на DuckDB и сверены. Поверх результатов выполнен анализ зарплат и регионов. Подробности, скриншоты и выводы в отчёте `report.pdf`.

## Что считается

Вакансии отбираются по правилу: `role = «Программист, разработчик»` и в названии (`name`) или ключевых навыках (`key_skills`) есть C#, C Sharp, .NET или dotnet (регистр не важен, русская «с#» заменяется на «c#»). Для отобранных вакансий считается частота навыков, пар и троек навыков.

Результат (6076 вакансий): 2082 уникальных навыка, 33585 пар, 228511 троек.

## Требования

- Python 3.9+
- RustFS 1.0.0, запущенный на 127.0.0.1:9000 (ключи в `config.py`), бакет `hh-lake`
- `pip install -r requirements.txt`

## Запуск

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python upload_to_s3.py          # загрузка it_vacancies_2024.csv в бакет hh-lake
./run_all.sh main               # MapReduce, DuckDB, сверка, тест отказа маппера, сводка
./run_all.sh analysis           # зарплаты и регионы
./run_all.sh profile            # профилирование, по 3 запуска
```

Вывод каждого шага сохраняется в `logs/`, результаты профилирования в `profiling/results.csv`.

## Файлы

| Файл | Назначение |
|---|---|
| `config.py` | адрес хранилища, ключи, имя бакета и объекта |
| `upload_to_s3.py` | загрузка датасета в S3 (multipart по 64 МБ) |
| `common.py` | правило отбора, чтение объекта кусками (HTTP Range) |
| `mapreduce_skills.py` | собственный MapReduce: map, combiner, shuffle, reduce, повторный запуск упавших мапперов |
| `skills.sql`, `duckdb_skills.py` | те же подсчёты на DuckDB (httpfs, чтение из S3) |
| `verify_results.py` | сравнение результатов MapReduce и DuckDB |
| `show_summary.py` | сводка и таблица роста числа пар и троек |
| `salary.sql`, `salary_analysis.py` | влияние навыков на зарплату |
| `regions.sql`, `regions_analysis.py` | сравнение Москвы, Санкт-Петербурга и Новосибирска |
| `profile_run.py` | замер времени и пиковой памяти (3 запуска) |
| `run_all.sh` | запуск всех шагов с сохранением логов |

## Параметры MapReduce

```bash
python mapreduce_skills.py --mode optimized   # или baseline
                           --chunk-mb 64 --block-mb 8 --workers 4 --reducers 4
                           --out mapreduce
                           --fail-chunk 5 --crash exception   # тест отказа маппера
```

## Где лежат результаты в бакете `hh-lake`

`results/mapreduce`, `results/mr_fail`, `results/mr_baseline`, `results/mr_optimized` (файлы `skills_1.csv`, `skills_2.csv`, `skills_3.csv`, `summary.json`), `results/duckdb`, `results/salary`, `results/regions`.

## Профилирование (среднее за 3 запуска)

| Вариант | Время, с | Пик памяти, МБ |
|---|---|---|
| MapReduce, baseline | 3,34 | 1036 |
| MapReduce, optimized | 2,13 | 464 |
| DuckDB | 1,24 | 384 |

Датасет `it_vacancies_2024.csv` в репозиторий не входит (808 МБ).

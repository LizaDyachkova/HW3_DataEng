INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint = '127.0.0.1:9000';
SET s3_access_key_id = 'hhadmin';
SET s3_secret_access_key = 'hhadmin-secret';
SET s3_region = 'us-east-1';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

CREATE OR REPLACE TABLE vac_all AS
SELECT
    vacancy_id,
    employer_id,
    name,
    area,
    experience,
    COALESCE(key_skills, '') AS key_skills,
    try_cast(salary_from AS DOUBLE) AS sf,
    try_cast(salary_to AS DOUBLE) AS st,
    currency,
    gross
FROM read_csv('s3://hh-lake/it_vacancies_2024.csv',
              header = true, delim = ',', quote = '"', escape = '"',
              all_varchar = true, strict_mode = false)
WHERE role = 'Программист, разработчик'
  AND (
        regexp_matches(replace(lower(COALESCE(name, '')), 'с#', 'c#'),
                       'c#|c ?sharp|\.net([^a-z]|$)|dotnet')
     OR regexp_matches(replace(lower(COALESCE(key_skills, '')), 'с#', 'c#'),
                       'c#|c ?sharp|\.net([^a-z]|$)|dotnet')
  );

CREATE OR REPLACE TABLE skills AS
SELECT DISTINCT vacancy_id, skill
FROM (
    SELECT vacancy_id,
           replace(lower(trim(unnest(string_split(key_skills, ';')))), 'с#', 'c#') AS skill
    FROM vac_all
)
WHERE skill <> '';

CREATE OR REPLACE TABLE skill_freq AS
SELECT skill, count(*) AS freq FROM skills GROUP BY skill;

CREATE OR REPLACE TABLE sal_raw AS
SELECT vacancy_id, employer_id, name, area, experience, gross,
       (CASE WHEN sf IS NOT NULL AND st IS NOT NULL THEN (sf + st) / 2
             WHEN sf IS NOT NULL THEN sf
             ELSE st END)
       * (CASE WHEN gross = 'true' THEN 0.87 ELSE 1.0 END) AS net
FROM vac_all
WHERE currency = 'RUR' AND (sf IS NOT NULL OR st IS NOT NULL);

CREATE OR REPLACE TABLE sal_clip AS
SELECT * FROM sal_raw WHERE net BETWEEN @MIN_SALARY@ AND @MAX_SALARY@;

CREATE OR REPLACE TABLE sal AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *, row_number() OVER (PARTITION BY employer_id, lower(name), net ORDER BY vacancy_id) AS rn
    FROM sal_clip
)
WHERE rn = 1;

CREATE OR REPLACE TABLE reg_summary AS
WITH nsk AS (SELECT vacancy_id, count(*) AS n FROM skills GROUP BY vacancy_id),
base AS (
    SELECT v.area, v.vacancy_id, v.experience, nsk.n AS n_skills, s.net
    FROM vac_all v
    LEFT JOIN nsk ON nsk.vacancy_id = v.vacancy_id
    LEFT JOIN sal s ON s.vacancy_id = v.vacancy_id
)
SELECT area,
       count(*) AS vacancies,
       count(n_skills) AS with_skills,
       round(avg(n_skills), 1) AS avg_skills,
       count(net) AS with_salary,
       round(median(net)) AS median_salary,
       round(quantile_cont(net, 0.25)) AS q1,
       round(quantile_cont(net, 0.75)) AS q3,
       round(100 * avg((experience = 'Нет опыта')::INT)) AS pct_no_exp,
       round(100 * avg((experience = 'Более 6 лет')::INT)) AS pct_6plus
FROM base
WHERE area IN (@REGIONS@)
GROUP BY area
UNION ALL
SELECT 'Вся выборка', count(*), count(n_skills), round(avg(n_skills), 1), count(net), round(median(net)),
       round(quantile_cont(net, 0.25)), round(quantile_cont(net, 0.75)),
       round(100 * avg((experience = 'Нет опыта')::INT)), round(100 * avg((experience = 'Более 6 лет')::INT))
FROM base;

CREATE OR REPLACE TABLE rs AS
SELECT s.vacancy_id, s.skill, v.area
FROM skills s JOIN vac_all v ON v.vacancy_id = s.vacancy_id
WHERE v.area IN (@REGIONS@);

CREATE OR REPLACE TABLE reg_top AS
WITH s1 AS (
    SELECT 1 AS size, r.area, r.skill AS combo, count(*) AS vacancies, count(s.net) AS with_salary,
           CASE WHEN count(s.net) >= 5 THEN round(median(s.net)) END AS median_salary,
           row_number() OVER (PARTITION BY r.area ORDER BY count(*) DESC, r.skill) AS rk
    FROM rs r LEFT JOIN sal s ON s.vacancy_id = r.vacancy_id
    GROUP BY r.area, r.skill
), s2 AS (
    SELECT 2 AS size, a.area, a.skill || ' + ' || b.skill AS combo, count(*) AS vacancies, count(s.net) AS with_salary,
           CASE WHEN count(s.net) >= 5 THEN round(median(s.net)) END AS median_salary,
           row_number() OVER (PARTITION BY a.area ORDER BY count(*) DESC, a.skill, b.skill) AS rk
    FROM rs a JOIN rs b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    LEFT JOIN sal s ON s.vacancy_id = a.vacancy_id
    GROUP BY a.area, a.skill, b.skill
), s3 AS (
    SELECT 3 AS size, a.area, a.skill || ' + ' || b.skill || ' + ' || c.skill AS combo, count(*) AS vacancies,
           count(s.net) AS with_salary,
           CASE WHEN count(s.net) >= 5 THEN round(median(s.net)) END AS median_salary,
           row_number() OVER (PARTITION BY a.area ORDER BY count(*) DESC, a.skill, b.skill, c.skill) AS rk
    FROM rs a
    JOIN rs b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    JOIN rs c ON a.vacancy_id = c.vacancy_id AND b.skill < c.skill
    LEFT JOIN sal s ON s.vacancy_id = a.vacancy_id
    GROUP BY a.area, a.skill, b.skill, c.skill
), u AS (
    SELECT * FROM s1 WHERE rk <= @TOP@
    UNION ALL SELECT * FROM s2 WHERE rk <= @TOP@
    UNION ALL SELECT * FROM s3 WHERE rk <= @TOP@
)
SELECT u.size, u.area, u.rk, u.combo, u.vacancies,
       round(100.0 * u.vacancies / g.with_skills, 1) AS pct,
       u.with_salary, u.median_salary
FROM u JOIN reg_summary g ON g.area = u.area;

CREATE OR REPLACE TABLE reg_lift AS
WITH tot AS (SELECT with_skills AS n FROM reg_summary WHERE area = 'Вся выборка'),
all1 AS (SELECT skill AS combo, count(*) AS n FROM skills GROUP BY skill),
reg1 AS (SELECT area, skill AS combo, count(*) AS n FROM rs GROUP BY area, skill),
all2 AS (
    SELECT a.skill || ' + ' || b.skill AS combo, count(*) AS n
    FROM skills a JOIN skills b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    GROUP BY a.skill, b.skill
),
reg2 AS (
    SELECT a.area, a.skill || ' + ' || b.skill AS combo, count(*) AS n
    FROM rs a JOIN rs b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    GROUP BY a.area, a.skill, b.skill
),
x AS (
    SELECT 1 AS size, r.area, r.combo, r.n AS in_region, a.n - r.n AS elsewhere FROM reg1 r JOIN all1 a USING (combo)
    UNION ALL
    SELECT 2, r.area, r.combo, r.n, a.n - r.n FROM reg2 r JOIN all2 a USING (combo)
)
SELECT * FROM (
    SELECT x.size, x.area, x.combo, x.in_region, x.elsewhere,
           round((x.in_region::DOUBLE / g.with_skills) / (x.elsewhere::DOUBLE / (t.n - g.with_skills)), 2) AS lift,
           row_number() OVER (PARTITION BY x.size, x.area
                              ORDER BY (x.in_region::DOUBLE / g.with_skills) / (x.elsewhere::DOUBLE / (t.n - g.with_skills)) DESC) AS rk
    FROM x JOIN reg_summary g ON g.area = x.area, tot t
    WHERE x.in_region >= @MIN_LIFT@ AND x.elsewhere >= @MIN_LIFT@
) WHERE rk <= @TOP@;

COPY (SELECT * FROM reg_summary) TO 's3://hh-lake/results/regions/regions_summary.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM reg_top ORDER BY area, size, rk) TO 's3://hh-lake/results/regions/regions_top_skills.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM reg_lift ORDER BY area, size, rk) TO 's3://hh-lake/results/regions/regions_lift.csv' (HEADER, DELIMITER ',');

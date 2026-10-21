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

CREATE OR REPLACE TABLE ss AS
SELECT s.vacancy_id, s.skill, v.net, v.area, v.experience, v.employer_id
FROM skills s JOIN sal v ON v.vacancy_id = s.vacancy_id;

CREATE OR REPLACE TABLE skill_med AS
SELECT skill, count(*) AS n, median(net) AS med FROM ss GROUP BY skill;

-- @@STAGE2@@

CREATE OR REPLACE TABLE pair_gain AS
WITH p AS (
    SELECT a.skill AS s1, b.skill AS s2, count(*) AS n, median(a.net) AS med_combo,
           count(DISTINCT a.employer_id) AS employers,
           avg((a.area = 'Москва')::INT) AS share_msk,
           avg((a.experience = '@JUNIOR@')::INT) AS share_junior
    FROM ss a JOIN ss b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    GROUP BY a.skill, b.skill
    HAVING count(*) >= @MIN_PAIR@ AND count(DISTINCT a.employer_id) >= @MIN_EMPLOYERS@
)
SELECT
    CASE WHEN fa.freq >= fb.freq THEN p.s1 ELSE p.s2 END AS base_skill,
    CASE WHEN fa.freq >= fb.freq THEN p.s2 ELSE p.s1 END AS extra_skill,
    p.n AS vacancies,
    p.employers AS employers,
    round(p.med_combo) AS median_combo,
    round(m.med) AS median_base,
    m.n AS vacancies_base,
    round(p.med_combo - m.med) AS gain,
    round(100 * (p.med_combo - m.med) / m.med, 1) AS gain_pct,
    round(100 * p.share_msk) AS pct_moscow,
    round(100 * p.share_junior) AS pct_junior
FROM p
JOIN skill_freq fa ON fa.skill = p.s1
JOIN skill_freq fb ON fb.skill = p.s2
JOIN skill_med m ON m.skill = CASE WHEN fa.freq >= fb.freq THEN p.s1 ELSE p.s2 END;

CREATE OR REPLACE TABLE triple_gain AS
WITH t AS (
    SELECT a.skill AS s1, b.skill AS s2, c.skill AS s3, count(*) AS n, median(a.net) AS med_combo,
           count(DISTINCT a.employer_id) AS employers,
           avg((a.area = 'Москва')::INT) AS share_msk,
           avg((a.experience = '@JUNIOR@')::INT) AS share_junior
    FROM ss a
    JOIN ss b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    JOIN ss c ON a.vacancy_id = c.vacancy_id AND b.skill < c.skill
    GROUP BY a.skill, b.skill, c.skill
    HAVING count(*) >= @MIN_TRIPLE@ AND count(DISTINCT a.employer_id) >= @MIN_EMPLOYERS@
), t2 AS (
    SELECT t.*,
           CASE WHEN fa.freq >= fb.freq AND fa.freq >= fc.freq THEN t.s1
                WHEN fb.freq >= fc.freq THEN t.s2
                ELSE t.s3 END AS base_skill
    FROM t
    JOIN skill_freq fa ON fa.skill = t.s1
    JOIN skill_freq fb ON fb.skill = t.s2
    JOIN skill_freq fc ON fc.skill = t.s3
)
SELECT
    t2.base_skill,
    CASE WHEN t2.base_skill = t2.s1 THEN t2.s2 || ' + ' || t2.s3
         WHEN t2.base_skill = t2.s2 THEN t2.s1 || ' + ' || t2.s3
         ELSE t2.s1 || ' + ' || t2.s2 END AS extra_skills,
    t2.n AS vacancies,
    t2.employers AS employers,
    round(t2.med_combo) AS median_combo,
    round(m.med) AS median_base,
    m.n AS vacancies_base,
    round(t2.med_combo - m.med) AS gain,
    round(100 * (t2.med_combo - m.med) / m.med, 1) AS gain_pct,
    round(100 * t2.share_msk) AS pct_moscow,
    round(100 * t2.share_junior) AS pct_junior
FROM t2 JOIN skill_med m ON m.skill = t2.base_skill;

COPY (SELECT * FROM pair_gain ORDER BY gain DESC) TO 's3://hh-lake/results/salary/pairs_gain.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM triple_gain ORDER BY gain DESC) TO 's3://hh-lake/results/salary/triples_gain.csv' (HEADER, DELIMITER ',');

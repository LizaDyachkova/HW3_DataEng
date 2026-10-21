INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint = '127.0.0.1:9000';
SET s3_access_key_id = 'hhadmin';
SET s3_secret_access_key = 'hhadmin-secret';
SET s3_region = 'us-east-1';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

CREATE OR REPLACE TABLE vac AS
SELECT
    vacancy_id,
    COALESCE(key_skills, '') AS key_skills
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
    FROM vac
)
WHERE skill <> '';

COPY (
    SELECT skill, count(*) AS count
    FROM skills
    GROUP BY skill
    ORDER BY count DESC, skill
) TO 's3://hh-lake/results/duckdb/skills_1.csv' (HEADER, DELIMITER ',');

COPY (
    SELECT a.skill AS skill_1, b.skill AS skill_2, count(*) AS count
    FROM skills a
    JOIN skills b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    GROUP BY a.skill, b.skill
    ORDER BY count DESC, skill_1, skill_2
) TO 's3://hh-lake/results/duckdb/skills_2.csv' (HEADER, DELIMITER ',');

COPY (
    SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, count(*) AS count
    FROM skills a
    JOIN skills b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
    JOIN skills c ON a.vacancy_id = c.vacancy_id AND b.skill < c.skill
    GROUP BY a.skill, b.skill, c.skill
    ORDER BY count DESC, skill_1, skill_2, skill_3
) TO 's3://hh-lake/results/duckdb/skills_3.csv' (HEADER, DELIMITER ',');

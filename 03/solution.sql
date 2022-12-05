DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(items text);

COPY input FROM '/Users/coleiain/aoc_2022/03/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, SUBSTRING(items, 1, LENGTH(items) / 2) as compartment1, SUBSTRING(items, LENGTH(items) / 2 + 1, LENGTH(items)) as compartment2
    FROM input
), lower_case as (
    SELECT chr(n) as item
    FROM GENERATE_SERIES(97, 97 + 25) AS t(n)
), upper_case as (
    SELECT chr(n) as item
    FROM GENERATE_SERIES(65, 65 + 25) AS t(n)
), series as (
    SELECT item
    FROM lower_case
    UNION ALL
    SELECT item
    FROM upper_case
), priorities as (
    SELECT row_number() over (ORDER BY 1) as priority, item
    FROM series
), distinct_types as (SELECT DISTINCT i.*, common_item, p.priority
    FROM indexed i
    CROSS JOIN LATERAL (
        SELECT compartment1.compartment1 as common_item
        FROM regexp_split_to_table(i.compartment1, '') as compartment1
        INNER JOIN regexp_split_to_table(i.compartment2, '') as compartment2 ON compartment1.compartment1 = compartment2.compartment2
    ) shared
    INNER JOIN priorities p ON p.item = common_item
)
SELECT SUM(priority) as priority
FROM distinct_types


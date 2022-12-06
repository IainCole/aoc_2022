DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(inputline text);

COPY input FROM '/Users/coleiain/aoc_2022/06/input';

WITH chars as (
    SELECT regexp_split_to_table(inputline, '') as char
    FROM input
), indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, char
    FROM chars
)
SELECT i.row_num
FROM indexed i
CROSS JOIN LATERAL (
    with prevs as (
        SELECT prev.row_num, char
        FROM indexed prev
        WHERE prev.row_num <= i.row_num
        ORDER BY prev.row_num DESC
        LIMIT 4
    )
    SELECT COUNT(DISTINCT char) as prev_distincts
    FROM prevs
) pd
WHERE prev_distincts = 4
ORDER BY row_num
LIMIT 1
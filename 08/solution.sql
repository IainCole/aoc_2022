DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(inputline text);

COPY input FROM '/Users/coleiain/aoc_2022/08/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, CAST(regexp_split_to_table(inputline, '') as int) as height
    FROM input
), grid as (
    SELECT row_num, ROW_NUMBER() OVER(PARTITION BY row_num ORDER BY 1) as col_num, height
    FROM indexed
), vl as (
    SELECT g.row_num, g.col_num, g.height
    FROM grid g
    LEFT JOIN grid gleft ON gleft.row_num = g.row_num AND gleft.col_num < g.col_num AND gleft.height >= g.height
    WHERE gleft.col_num IS NULL
), vr as (
    SELECT g.row_num, g.col_num, g.height
    FROM grid g
    LEFT JOIN grid gright ON gright.row_num = g.row_num AND gright.col_num > g.col_num AND gright.height >= g.height
    WHERE gright.col_num IS NULL
), vdown as (
    SELECT g.row_num, g.col_num, g.height
    FROM grid g
    LEFT JOIN grid gdown ON gdown.row_num > g.row_num AND gdown.col_num = g.col_num AND gdown.height >= g.height
    WHERE gdown.row_num IS NULL
), vup as (
    SELECT g.row_num, g.col_num, g.height
    FROM grid g
    LEFT JOIN grid gup ON gup.row_num < g.row_num AND gup.col_num = g.col_num AND gup.height >= g.height
    WHERE gup.row_num IS NULL
), vany as (
    SELECT * FROM vl
    UNION ALL
    SELECT * FROM vr
    UNION ALL
    SELECT * FROM vdown
    UNION ALL
    SELECT * FROM vup
), distinct_cells as (
    SELECT DISTINCT row_num, col_num, height
    FROM vany
    ORDER BY row_num, col_num
)
SELECT COUNT(*) as trees
FROM distinct_cells



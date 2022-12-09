DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(inputline text);

COPY input FROM '/Users/coleiain/aoc_2022/08/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, CAST(regexp_split_to_table(inputline, '') as int) as height
    FROM input
), grid as (
    SELECT row_num, ROW_NUMBER() OVER(PARTITION BY row_num ORDER BY 1) as col_num, height
    FROM indexed
), limits as (
    SELECT MAX(row_num) as max_rows, MAX(col_num) as max_cols
    FROM grid
), vl as (
    SELECT g.row_num, g.col_num, g.height, COALESCE(score.distance, g.col_num - 1) as score
    FROM grid g
    LEFT JOIN LATERAL (
        SELECT g.col_num - l.col_num as distance
        FROM grid l
        WHERE g.row_num = l.row_num AND l.col_num < g.col_num AND l.height >= g.height
        ORDER BY l.col_num DESC
        LIMIT 1
    ) score ON true
), vr as (
    SELECT g.row_num, g.col_num, g.height, COALESCE(score.distance, lim.max_cols - g.col_num) as score
    FROM grid g
    CROSS JOIN limits lim
    LEFT JOIN LATERAL (
        SELECT l.col_num - g.col_num as distance
        FROM grid l
        WHERE g.row_num = l.row_num AND l.col_num > g.col_num AND l.height >= g.height
        ORDER BY l.col_num
        LIMIT 1
    ) score ON true
), vu as (
    SELECT g.row_num, g.col_num, g.height, COALESCE(score.distance, g.row_num - 1) as score
    FROM grid g
    LEFT JOIN LATERAL (
        SELECT g.row_num - l.row_num as distance
        FROM grid l
        WHERE g.col_num = l.col_num AND l.row_num < g.row_num AND l.height >= g.height
        ORDER BY l.row_num DESC
        LIMIT 1
    ) score ON true
), vd as (
    SELECT g.row_num, g.col_num, g.height, COALESCE(score.distance, lim.max_rows - g.row_num) as score
    FROM grid g
    CROSS JOIN limits lim
    LEFT JOIN LATERAL (
        SELECT l.row_num - g.row_num as distance
        FROM grid l
        WHERE g.col_num = l.col_num AND l.row_num > g.row_num AND l.height >= g.height
        ORDER BY l.row_num
        LIMIT 1
    ) score ON true
), scores as (
    SELECT l.row_num, l.col_num, l.height, l.score as leftscore, r.score as rightscore, u.score as upscore, d.score as downscore
    FROM vl l
    INNER JOIN vr r ON r.row_num = l.row_num AND r.col_num = l.col_num
    INNER JOIN vu u ON u.row_num = l.row_num AND u.col_num = l.col_num
    INNER JOIN vd d ON d.row_num = l.row_num AND d.col_num = l.col_num
)
SELECT MAX(leftscore*rightscore*upscore*downscore)
FROM scores




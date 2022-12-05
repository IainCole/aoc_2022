DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(round text);

COPY input FROM '/Users/coleiain/aoc_2022/02/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, SPLIT_PART(round, ' ', 1) as them, SPLIT_PART(round, ' ', 2) as ending
    FROM input
), shapes as (
    SELECT 'Rock' as shape, 1 as score
    UNION ALL
    SELECT 'Paper', 2
    UNION ALL
    SELECT 'Scissors', 3
), codes as (
    SELECT 'A' as code, 'Rock' as shape
    UNION ALL
    SELECT 'B', 'Paper'
    UNION ALL
    SELECT 'C', 'Scissors'
), ends as (
    SELECT 'X' as code, 0 as score
    UNION ALL
    SELECT 'Y', 3
    UNION ALL
    SELECT 'Z', 6
), outcomes as (
    SELECT s.shape as p1s, e.score, CASE
        WHEN e.score = 3 THEN s.shape
        WHEN s.shape = 'Rock' AND e.score = 0 THEN 'Scissors'
        WHEN s.shape = 'Rock' AND e.score = 6 THEN 'Paper'
        WHEN s.shape = 'Paper' AND e.score = 0 THEN 'Rock'
        WHEN s.shape = 'Paper' AND e.score = 6 THEN 'Scissors'
        WHEN s.shape = 'Scissors' AND e.score = 0 THEN 'Paper'
        WHEN s.shape = 'Scissors' AND e.score = 6 THEN 'Rock'
    END as p2s
    FROM shapes s
    CROSS JOIN ends e
), rounds as (
    SELECT row_num, theirshape.shape as them, o.p2s as me, s.score + o.score as score
    FROM indexed i
    INNER JOIN codes theirshape ON theirshape.code = i.them
    INNER JOIN ends e ON e.code = i.ending
    INNER JOIN outcomes o ON o.p1s = theirshape.shape AND o.score = e.score
    INNER JOIN shapes s ON o.p2s = s.shape
)
SELECT SUM(score) as score
FROM rounds
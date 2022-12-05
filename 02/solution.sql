DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(round text);

COPY input FROM '/Users/coleiain/aoc_2022/02/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, SPLIT_PART(round, ' ', 1) as them, SPLIT_PART(round, ' ', 2) as me
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
    UNION ALL
    SELECT 'X', 'Rock'
    UNION ALL
    SELECT 'Y', 'Paper'
    UNION ALL
    SELECT 'Z', 'Scissors'
), outcomes as (
    SELECT s1.shape as p1s, s2.shape as p2s, CASE
        WHEN s1.shape = s2.shape THEN 3
        WHEN s1.shape = 'Rock' AND s2.shape = 'Paper' THEN 0
        WHEN s1.shape = 'Rock' AND s2.shape = 'Scissors' THEN 6
        WHEN s1.shape = 'Paper' AND s2.shape = 'Scissors' THEN 0
        WHEN s1.shape = 'Paper' AND s2.shape = 'Rock' THEN 6
        WHEN s1.shape = 'Scissors' AND s2.shape = 'Rock' THEN 0
        WHEN s1.shape = 'Scissors' AND s2.shape = 'Paper' THEN 6
    END + s1.score as score
    FROM shapes s1
    CROSS JOIN shapes s2
), rounds as (
    SELECT row_num, myshape.shape as me, theirshape.shape as them, o.score
    FROM indexed i
        INNER JOIN codes myshape ON myshape.code = i.me
        INNER JOIN codes theirshape ON theirshape.code = i.them
        INNER JOIN outcomes o ON o.p1s = myshape.shape AND o.p2s = theirshape.shape
)
SELECT SUM(score) as score
FROM rounds
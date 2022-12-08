DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(inputline text);

COPY input FROM '/Users/coleiain/aoc_2022/07/input';

WITH recursive indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, inputline
    FROM input
), cmdgroup as (
    SELECT i.*, r.row_num as cmd_num
    FROM indexed i
    LEFT JOIN LATERAL (
        SELECT row_num
        FROM indexed
        WHERE row_num = (SELECT row_num FROM indexed WHERE row_num <= i.row_num AND SUBSTRING(inputline, 1, 1) = '$' ORDER BY row_num DESC LIMIT 1)
    ) r ON true
), commands (row_num, command, abs_path) as (
    SELECT row_num, SUBSTRING(inputline, 3) as command, ''
    FROM cmdgroup cg
    WHERE row_num = 1
    UNION ALL
    SELECT cg.row_num, SUBSTRING(cg.inputline, 3) as command,
    CASE
        WHEN SUBSTRING(inputline, 3, 2) = 'ls' THEN c.abs_path
        WHEN SUBSTRING(inputline, 3, 2) = 'cd' THEN
            CASE
                WHEN SUBSTRING(inputline, 6) = '..'
                THEN SUBSTRING(c.abs_path, 1, LENGTH(c.abs_path) - POSITION('/' IN REVERSE(c.abs_path)))
                ELSE concat(c.abs_path, '/', SUBSTRING(inputline, 6))
            END
    END
    FROM commands c
    CROSS JOIN LATERAL (
        SELECT *
        FROM cmdgroup cg
        WHERE cg.row_num > c.row_num AND SUBSTRING(inputline, 1, 1) = '$'
        LIMIT 1
    ) cg
), objects as (
    SELECT cg.row_num,
        CASE WHEN SUBSTRING(inputline, 1, 3) = 'dir' THEN 'dir' ELSE 'file' END as type,
        CASE
            WHEN SUBSTRING(inputline, 1, 3) = 'dir' THEN SUBSTRING(inputline, 5)
            ELSE SUBSTRING(inputline, POSITION(' ' in inputline) + 1) END as name,
        CASE
            WHEN SUBSTRING(inputline, 1, 3) != 'dir' THEN CAST(LEFT(inputline, POSITION(' ' in inputline)) as int)
            ELSE 0 END as size,
        cg.cmd_num,
        CASE WHEN c.abs_path = '' THEN '/' ELSE c.abs_path END as path
    FROM cmdgroup cg
    INNER JOIN commands c ON c.row_num = cg.cmd_num
    WHERE SUBSTRING(cg. inputline, 1, 1) != '$'
), dirs as (
    SELECT o.*
    FROM objects o
    WHERE type = 'dir'
), dir_sizes as (
    SELECT t.row_num, t.type, t.name, t.path, SUM(o.size) as size
    FROM dirs t
    INNER JOIN objects o ON o.type = 'file' and o.path LIKE t.path || '/' || t.name || '%'
    GROUP BY t.row_num, t.type, t.name, t.path
)
SELECT SUM(size)
FROM dir_sizes
WHERE size < 100000
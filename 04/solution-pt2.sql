DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(assignments text);

COPY input FROM '/Users/coleiain/aoc_2022/04/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, assignments
    FROM input
), grouped as (
    SELECT i.row_num, assignments.elf, CAST(split_part(assignments.range, '-', 1) as int) as lower, CAST(split_part(assignments.range, '-', 2) as int) as upper
FROM indexed i
    CROSS JOIN LATERAL (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as elf, elf.elf as range
    FROM regexp_split_to_table((SELECT assignments FROM indexed e1  WHERE e1.row_num = i.row_num), ',') as elf
    ) as assignments
), overlapping_ranges as (
    SELECT DISTINCT row_num, ranges.*
    FROM grouped g
    CROSS JOIN LATERAL (
        SELECT e1.lower as e1lower, e1.upper as e1upper, e2.lower as e2lower, e2.upper as e2upper
        FROM grouped e1
        INNER JOIN grouped e2 ON e2.row_num = g.row_num AND e2.elf = 2
        WHERE e1.row_num = g.row_num AND e1.elf = 1
        ) as ranges
    WHERE
    (e1lower <= e2upper AND e1upper >= e2lower)
    OR
    (e2lower <= e1upper AND e2upper >= e1lower)
)
SELECT COUNT(*) as num_overlaps
FROM overlapping_ranges
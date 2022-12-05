DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(items text);

COPY input FROM '/Users/coleiain/aoc_2022/03/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, items
    FROM input
), rowcount as (
   SELECT CAST(COUNT(*) as int) as rows
   FROM indexed
), grouped as (
  SELECT i.row_num, NTILE(rc.rows / 3) OVER (ORDER BY i.row_num) as group_num, i.items
  FROM indexed i
  CROSS JOIN rowcount rc
), sequenced as (
    SELECT g.row_num, g.group_num, ROW_NUMBER() OVER(PARTITION BY g.group_num ORDER BY g.row_num) as elfnum, g.items
    FROM grouped g
    CROSS JOIN rowcount rc
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
), groups_only as (
    SELECT DISTINCT group_num
    FROM sequenced
), common_items as (
    SELECT DISTINCT group_num, shared.common_item
    FROM groups_only g
    CROSS JOIN LATERAL (
        SELECT elf1.elf1 as common_item
        FROM regexp_split_to_table((SELECT items FROM sequenced e1 WHERE e1.group_num = g.group_num AND e1.elfnum = 1), '') as elf1
        INNER JOIN regexp_split_to_table((SELECT items FROM sequenced e2 WHERE e2.group_num = g.group_num AND e2.elfnum = 2), '') as elf2 ON elf2.elf2 = elf1.elf1
        INNER JOIN regexp_split_to_table((SELECT items FROM sequenced e3 WHERE e3.group_num = g.group_num AND e3.elfnum = 3), '') as elf3 ON elf3.elf3 = elf2.elf2
    ) shared
    INNER JOIN priorities p ON p.item = shared.common_item
)
SELECT SUM(p.priority) as priority
FROM common_items c
INNER JOIN priorities p on c.common_item = p.item
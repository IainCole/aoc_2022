DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(inputline text);

COPY input FROM '/Users/coleiain/aoc_2022/05/input';

WITH recursive indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, inputline
    FROM input
), break as (
    SELECT row_num
    FROM indexed
    WHERE inputline = ''
    LIMIT 1
), boxes as (
    SELECT *
    FROM indexed
    WHERE row_num < (select row_num from break)
    ORDER BY row_num desc
), chars as (
    SELECT row_num, regexp_split_to_table(inputline, '') as char
    FROM boxes
), with_idx as (
    SELECT ROW_NUMBER() OVER (ORDER BY 1) as idx, row_num, char
    FROM chars
), with_col as (
    SELECT row_num, ROW_NUMBER() OVER (PARTITION BY row_num ORDER BY idx) as col_num, char
    FROM with_idx
    ORDER BY row_num desc, col_num
), without_cruft as (
    SELECT *
    FROM with_col
    WHERE (col_num - 2) % 4 = 0
), recolumned as (
    SELECT wc.row_num, ROW_NUMBER() OVER (PARTITION BY wc.row_num ORDER BY wc.col_num) as col_num, char
    FROM without_cruft wc
    WHERE row_num < 9
    ORDER BY wc.row_num desc, wc.col_num
), stacks as (
    SELECT DISTINCT rc.col_num, concat.stack
    FROM recolumned rc
             CROSS JOIN LATERAL (
        SELECT TRIM(string_agg(agg.char, '' ORDER BY agg.row_num asc)) as stack
        FROM recolumned agg
        WHERE agg.col_num = rc.col_num
        GROUP BY agg.col_num
        ) concat
    ORDER BY col_num
), inline_stacks as (
    SELECT string_agg(s.stack, '|' ORDER BY s.col_num) as stacks
    FROM stacks s
), instructions as (
    SELECT ROW_NUMBER() OVER (ORDER BY 1) as instruction_num,
           CAST(SUBSTRING(inputline, POSITION('move' in inputline) + 5, (POSITION('from' in inputline) - 1) - (POSITION('move' in inputline) + 5 )) as int) as move,
           CAST(SUBSTRING(inputline, POSITION('from' in inputline) + 5, (POSITION('to' in inputline) - 1) - (POSITION('from' in inputline) + 5 )) as int) as from_col,
           CAST(SUBSTRING(inputline, POSITION('to' in inputline) + 3, LENGTH(inputline) - (POSITION('move' in inputline) + 5 )) as int) as to_col
    FROM indexed
    WHERE row_num > (select row_num from break)
    ORDER BY row_num asc
), operation (instruction_num, move, from_col, to_col, iterstacks) as (
    SELECT instruction_num, i.move, i.from_col, i.to_col, ns.stacks
    FROM inline_stacks s
             CROSS JOIN LATERAL (
        SELECT *
        FROM instructions
        WHERE instruction_num = 1
        ) i
             CROSS JOIN LATERAL (
        with stacks as (
            SELECT i.from_col, i.to_col, i.move, regexp_split_to_table(s.stacks, '\|') as stack
        ), numbered_stacks as (
            SELECT ROW_NUMBER() OVER (ORDER BY 1) as row_num, stack
            FROM stacks
        ), fromstack as (
            select i.move, stack
            FROM numbered_stacks
            WHERE row_num = from_col
        ), tostack as (
            select stack
            FROM numbered_stacks
            WHERE row_num = to_col
        ), modifications as (
            SELECT CONCAT(SUBSTRING(fs.stack, 1, fs.move), ts.stack) as newto, RIGHT(fs.stack, LENGTH(fs.stack) - (fs.move)) as newfrom
            FROM fromstack fs
                     CROSS JOIN tostack ts
        )
        SELECT string_agg(
                       CASE
                           WHEN i.from_col = s.row_num THEN mods.newfrom
                           WHEN i.to_col = s.row_num THEN mods.newto
                           ELSE s.stack
                           END
                   , '|' ORDER BY s.row_num
                   ) as stacks
        FROM numbered_stacks s
                 CROSS JOIN modifications mods
        ) ns
    UNION ALL
    SELECT i.instruction_num, i.move, i.from_col, i.to_col, ns.stacks
    FROM operation o
             CROSS JOIN LATERAL (
        SELECT *
        FROM instructions
        WHERE instruction_num = o.instruction_num + 1
        ) i
             CROSS JOIN LATERAL (
        with stacks as (
            SELECT regexp_split_to_table(o.iterstacks, '\|') as stack
        ), numbered_stacks as (
            SELECT ROW_NUMBER() OVER (ORDER BY 1) as row_num, stack
            FROM stacks
        ), fromstack as (
            select i.move, stack
            FROM numbered_stacks
            WHERE row_num = i.from_col
        ), tostack as (
            select stack
            FROM numbered_stacks
            WHERE row_num = i.to_col
        ), modifications as (
            SELECT CONCAT(SUBSTRING(fs.stack, 1, fs.move), ts.stack) as newto, RIGHT(fs.stack, LENGTH(fs.stack) - (fs.move)) as newfrom
            FROM fromstack fs
                     CROSS JOIN tostack ts
        )
        SELECT string_agg(
                       CASE
                           WHEN i.from_col = s.row_num THEN mods.newfrom
                           WHEN i.to_col = s.row_num THEN mods.newto
                           ELSE s.stack
                           END
                   , '|' ORDER BY s.row_num
                   ) as stacks
        FROM numbered_stacks s
                 CROSS JOIN modifications mods
        ) ns
), lastop as (
    SELECT *
    FROM operation
    ORDER BY instruction_num DESC
    LIMIT 1
), laststacks as (
    SELECT regexp_split_to_table(iterstacks, '\|') as stack
    FROM lastop
)
SELECT string_agg(SUBSTRING(stack, 1, 1), '') as top_boxes
FROM laststacks




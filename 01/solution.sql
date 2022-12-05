DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(calories text);

COPY input FROM '/Users/coleiain/aoc_2022/01/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, calories
    FROM input
), segments as (SELECT i.*, COALESCE(grp.row_num, 1) as grp_num
                FROM indexed i
                         LEFT JOIN LATERAL (SELECT row_num
                                            FROM indexed i2
                                            WHERE i2.row_num <= i.row_num AND i2.calories = ''
                                            ORDER BY row_num DESC
                                            LIMIT 1) grp ON 1 = 1
                WHERE calories != ''
), total_calories as (SELECT grp_num, SUM(CAST(calories as int)) as calories
                      FROM segments
                      GROUP BY grp_num
)
SELECT calories
FROM total_calories
ORDER BY calories DESC
LIMIT 1
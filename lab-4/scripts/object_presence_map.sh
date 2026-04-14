#!/bin/bash

echo "OBJECT A B G"
/home/mateusz/.duckdb/cli/latest/duckdb -c  "
SELECT
    object_id AS OBJECT,
    MAX(CASE WHEN filename LIKE '%satellite_A%' THEN 'X' ELSE '-' END) AS A,
    MAX(CASE WHEN filename LIKE '%satellite_B%' THEN 'X' ELSE '-' END) AS B,
    MAX(CASE WHEN filename LIKE '%ground_station%' THEN 'X' ELSE '-' END) AS G
FROM read_csv_auto('../providers/*/observations.csv', filename=true)
GROUP BY object_id
ORDER BY object_id;
" -noheader

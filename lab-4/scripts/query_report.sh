#!/bin/bash

OBJ_ID=${1:-"OBJ-003"}
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_FILE="../reports/query_report_${TIMESTAMP}.txt"

echo "DATA SPACE QUERY REPORT" > "$REPORT_FILE"
echo "-----------------------" >> "$REPORT_FILE"
echo "Generated at: $(date +"%Y-%m-%d %H:%M:%S")" >> "$REPORT_FILE"

echo -e "\n[GLOBAL STATISTICS]" >> "$REPORT_FILE"
TOTAL_OBS=$(/home/mateusz/.duckdb/cli/latest/duckdb -c  "SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv');" -noheader)
DISTINCT_OBJS=$(/home/mateusz/.duckdb/cli/latest/duckdb -c "SELECT COUNT(DISTINCT object_id) FROM read_csv_auto('../providers/*/observations.csv');" -noheader)
echo "Total observations: $TOTAL_OBS" >> "$REPORT_FILE"
echo "Distinct objects: $DISTINCT_OBJS" >> "$REPORT_FILE"

echo -e "\n[OBJECT ANALYSIS: $OBJ_ID]" >> "$REPORT_FILE"
echo "Providers containing object:" >> "$REPORT_FILE"
/home/mateusz/.duckdb/cli/latest/duckdb -c "
SELECT DISTINCT '- ' || replace(replace(filename, '../providers/', ''), '/observations.csv', '') 
FROM read_csv_auto('../providers/*/observations.csv', filename=true) 
WHERE object_id='$OBJ_ID';
" -noheader >> "$REPORT_FILE"

OBJ_COUNT=$(/home/mateusz/.duckdb/cli/latest/duckdb -c "SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv') WHERE object_id='$OBJ_ID';" -noheader)
echo "Total observations: $OBJ_COUNT" >> "$REPORT_FILE"

echo -e "\n[FEDERATED QUERY COMPARISON]" >> "$REPORT_FILE"
FULL_RESULT=$(/home/mateusz/.duckdb/cli/latest/duckdb -c  "SELECT COUNT(*) FROM read_csv_auto('../providers/*/observations.csv') WHERE object_id='$OBJ_ID';" -noheader)
FED_RESULT=$(/home/mateusz/.duckdb/cli/latest/duckdb -c  "SELECT COUNT(*) FROM read_csv_auto(['../providers/satellite_A/observations.csv', '../providers/satellite_B/observations.csv']) WHERE object_id='$OBJ_ID';" -noheader)

echo "FULL RESULT: $FULL_RESULT" >> "$REPORT_FILE"
echo "FEDERATED RESULT: $FED_RESULT" >> "$REPORT_FILE"

if [[ "$FULL_RESULT" == "$FED_RESULT" ]]; then
    echo "COMPLETE: YES" >> "$REPORT_FILE"
else
    echo "COMPLETE: NO" >> "$REPORT_FILE"
fi

echo -e "\n[SCHEMA VALIDATION]" >> "$REPORT_FILE"

CHECK_VELOCITY=$(/home/mateusz/.duckdb/cli/latest/duckdb -c "DESCRIBE SELECT * FROM '../providers/ground_station/observations.csv';" | grep -c "velocity")

if [ "$CHECK_VELOCITY" -eq 0 ]; then
    echo "Schema consistency: INCONSISTENT (Missing column 'velocity' in ground_station)" >> "$REPORT_FILE"
else
    echo "Schema consistency: CONSISTENT" >> "$REPORT_FILE"
fi

echo "Raport został wygenerowany: $REPORT_FILE"

#!/bin/bash
echo "DATA SPACE REPORT" > ../reports/data_space_report.txt
echo "Total datasets: $(./list_datasets.sh | wc -l | tr -d ' ')" >> ../reports/data_space_report.txt
echo "Total records: $(cat ../providers/*/*.csv | wc -l | tr -d ' ')" >> ../reports/data_space_report.txt
echo "Objects found for query OBJ-003: $(./search_distributed.sh OBJ-003 | wc -l | tr -d ' ')" >> ../reports/data_space_report.txt
echo "Consistency check: $(./compare_queries.sh OBJ-003 | grep "Consistency check" | cut -d ' ' -f3)" >> ../reports/data_space_report.txt

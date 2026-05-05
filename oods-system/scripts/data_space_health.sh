#!/bin/bash
echo "DATA SPACE HEALTH REPORT" > ../reports/data_space_health.txt

echo "Total datasets: $(./list_datasets.sh | wc -l | tr -d ' ')" >> ../reports/data_space_health.txt

missing_meta=$(./check_metadata_coverage.sh | grep "Missing metadata" | wc -l | tr -d ' ')
echo "Datasets missing metadata: $missing_meta" >> ../reports/data_space_health.txt

empty_datasets=$(./check_metadata_consistency.sh | grep "is empty" | wc -l | tr -d ' ')
echo "Empty datasets: $empty_datasets" >> ../reports/data_space_health.txt

inconsistent=$(./find_inconsistent_records.sh | grep "Inconsistent data" | wc -l | tr -d ' ')
echo "Inconsistent datasets (records): $inconsistent" >> ../reports/data_space_health.txt

comp=$(./check_query_completeness.sh OBJ-003 | grep -q "yes" && echo "yes" || echo "no")
echo "Federated queries complete: $comp" >> ../reports/data_space_health.txt


#!/bin/bash

echo "DATA SPACE REPORT"
echo "Total datasets: $(./list_datasets.sh | wc -l)"
echo "Total records: $(cat ../providers/*/*.csv | wc -l)"
echo "Objects found for query OBJ-003: $(./search_distributed.sh OBJ-003 | wc -l)"
echo "Consistency check: no"

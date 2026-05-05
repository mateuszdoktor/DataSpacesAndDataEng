#!/bin/bash
cat ../providers/*/*.csv | grep -v timestamp | cut -d',' -f1,2 | sort | uniq -d > dup_keys.txt
for key in $(cat dup_keys.txt)
do
  lines=$(grep -h "$key" ../providers/*/*.csv | sort | uniq | wc -l | tr -d ' ')
  if [ "$lines" -gt 1 ]; then
    echo "Inconsistent data for: $key"
    grep "$key" ../providers/*/*.csv
  fi
done
rm dup_keys.txt

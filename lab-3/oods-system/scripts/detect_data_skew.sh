#!/bin/bash
for file in ../providers/*/*.csv
do
  provider=$(basename $(dirname $file))
  count=$(cat $file | grep -v timestamp | wc -l | tr -d ' ')
  echo "$count $provider" >> counts.txt
done

min=$(sort -n counts.txt | head -n 1 | cut -d' ' -f1)
min_prov=$(sort -n counts.txt | head -n 1 | cut -d' ' -f2)

max=$(sort -n counts.txt | tail -n 1 | cut -d' ' -f1)
max_prov=$(sort -n counts.txt | tail -n 1 | cut -d' ' -f2)

diff=$((max - min))

echo "Largest dataset: $max_prov ($max records)"
echo "Smallest dataset: $min_prov ($min records)"
echo "Difference: $diff records"
rm counts.txt

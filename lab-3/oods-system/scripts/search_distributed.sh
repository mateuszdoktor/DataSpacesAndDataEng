#!/bin/bash
for file in ../providers/*/*.csv
do
  grep $1 $file
done
echo "Total: $(grep $1 ../providers/*/*.csv | wc -l)"

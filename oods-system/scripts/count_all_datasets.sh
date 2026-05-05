#!/bin/bash
for file in ../providers/*/*.csv
do
  provider=$(basename $(dirname $file))
  count=$(cat $file | wc -l)
  echo "$provider: $count"
done

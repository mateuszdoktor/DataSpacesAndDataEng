#!/bin/bash
for file in ../providers/*/*.csv
do
  provider=$(basename $(dirname $file))
  filename=$(basename $file)
  central_file="../central_repository/${provider}_${filename}"
  
  if [ ! -f "$central_file" ]; then
    echo "Dataset not synchronized to central repository: $file"
    continue
  fi
  
  diff_result=$(diff $file $central_file)
  if [ "$diff_result" != "" ]; then
    echo "Stale central data for: $provider"
  fi
done

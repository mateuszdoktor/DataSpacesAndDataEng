#!/bin/bash
for metadata in ../metadata_catalog/*.json
do
  data_path=$(grep "data_path" $metadata | cut -d '"' -f4)
  if [ -z "$data_path" ]; then
    continue
  fi
  
  if [ ! -f "../$data_path" ]; then
    echo "Inconsistent: Dataset $data_path does not exist (from $metadata)"
  else
    records=$(cat "../$data_path" | grep -v timestamp | wc -l | tr -d ' ')
    if [ "$records" -eq 0 ]; then
      echo "Inconsistent: Dataset $data_path is empty (from $metadata)"
    fi
  fi
done

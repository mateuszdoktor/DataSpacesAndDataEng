#!/bin/bash
for metadata in ../metadata_catalog/*.json
do
  data_path=$(grep "data_path" $metadata | cut -d '"' -f4)
  grep $1 ../$data_path 2>/dev/null
done

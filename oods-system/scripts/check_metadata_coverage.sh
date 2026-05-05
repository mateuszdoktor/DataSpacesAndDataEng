#!/bin/bash
for file in ../providers/*/*.csv
do
  rel_path="providers/$(basename $(dirname $file))/$(basename $file)"
  if ! grep -q "$rel_path" ../metadata_catalog/*.json 2>/dev/null; then
    echo "Missing metadata for: $rel_path"
  fi
done

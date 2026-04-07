#!/bin/bash
get_paths(){
grep "data_path" ../metadata_catalog/*.json | cut -d ":" -f3 | cut -d '"' -f2
}

for file in $(get_paths)
do
 echo "$(grep $1 ../$file)" 
done


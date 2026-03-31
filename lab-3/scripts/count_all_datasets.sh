#!/bin/bash
count_rows(){
 echo "$1: $(cat $1 | wc -l)"
}
for file in ../providers/*/*.csv
do
 count_rows $file
done

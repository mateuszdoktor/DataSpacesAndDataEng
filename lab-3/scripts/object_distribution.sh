#!/bin/bash
get_occurrences(){
 cat $1 | cut -d ',' -f2
}

all_occurrences(){
for file in ../providers/*/*.csv
do
 get_occurrences $1 | uniq -c
done
}

echo "$all_ocurrences"

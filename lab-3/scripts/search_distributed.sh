#!/bin/bash

matching_records(){
 grep $1 $2
}

all_matching_records(){
for file in ../providers/*/*.csv 
do
 matching_records $1 $file
done
}

all_matching_records $1


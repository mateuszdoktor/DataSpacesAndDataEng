#!/bin/bash
dist=$(./search_distributed.sh $1 | grep $1 | wc -l)
fed=$(./search_federated.sh $1 | grep $1 | wc -l)

echo "Distributed results: $dist"
echo "Federated results: $fed"

if [ "$dist" == "$fed" ]; then
  echo "Consistency check: yes"
else
  echo "Consistency check: no"
fi

#!/bin/bash
dist=$(./search_distributed.sh $1 | grep $1 | wc -l | tr -d ' ')
fed=$(./search_federated.sh $1 | grep $1 | wc -l | tr -d ' ')

if [ "$dist" == "$fed" ]; then
  echo "Federated access returns complete results: yes"
else
  echo "Federated access returns complete results: no ($fed out of $dist)"
fi

#!/bin/bash

echo "Distributed search results"
./search_distributed.sh $1
echo "number of results $(./search_distributed.sh $1 | wc -l)"
echo -n "Federated search results"
./search_federated.sh $1
echo "number of results $(./search_federated.sh $1 | wc -l)"

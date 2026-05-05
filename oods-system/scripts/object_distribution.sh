#!/bin/bash
echo "Object distribution across providers:"
cat ../providers/*/*.csv | grep -v timestamp | cut -d',' -f2 | sort | uniq -c

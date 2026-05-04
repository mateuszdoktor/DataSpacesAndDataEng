#!/bin/bash
total_prov=$(ls -d ../providers/*/ | wc -l | tr -d ' ')
cat ../providers/*/*.csv | grep -v timestamp | cut -d',' -f2 | sort | uniq > all_objs.txt

echo "Objects in all providers:"
for obj in $(cat all_objs.txt)
do
  count=$(grep -l "$obj" ../providers/*/*.csv | wc -l | tr -d ' ')
  if [ "$count" -eq "$total_prov" ]; then
    echo $obj
  fi
done

echo ""
echo "Objects in only one provider:"
for obj in $(cat all_objs.txt)
do
  count=$(grep -l "$obj" ../providers/*/*.csv | wc -l | tr -d ' ')
  if [ "$count" -eq 1 ]; then
    echo $obj
  fi
done
rm all_objs.txt

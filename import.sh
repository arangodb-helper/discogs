#!/bin/bash

for i in masters artists labels releases; do
  ./build/bin/arangoimp ../data/$i.json --type json --collection $i --create-collection true
done

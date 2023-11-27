#!/bin/bash

echo "Scanning for lines with extra separators"

# Identifies the lines with extra separators
for f in data/test_result_*.txt
    do echo "Scanning $f ..."
    awk -F"|" 'NF > 14 {print $0}' < $f
    done

echo "Correcting known bad separators"

# Replace pipes with numerals
sed -i -e 's/TOYOTA|Estima |||/TOYOTA|Estima II|/g' data/test_result_2015.txt
sed -i -e 's/TOYOTA|Estima |||/TOYOTA|Estima II|/g' data/test_result_2016.txt
#!/bin/bash

type=$1

if [ -e times-dvt-$type.txt ]
then
    rm times-dvt-$type.txt
fi

for i in `seq 1 10`; do
    bin/cli.sh conf/dvt.json > temp.txt && python3 parser.py dump temp.txt >> times-dvt-$type.txt
done
rm temp.txt
python3 parser.py compute times-dvt-$type.txt > metrics-dvt-$type.txt

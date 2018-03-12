#! /usr/bin/env bash

set -x

git checkout firas/func-dependencies-master
./build.sh core
bin/cli.sh conf/fed_disbursements.json > func-dependencies-fed_disbursements.txt

git checkout firas/scratch
./build.sh core
bin/cli.sh conf/fed_disbursements.json > master-fed_disbursements.txt

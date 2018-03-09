#! /usr/bin/env bash

set -x

git checkout firas/func-dependencies-master
./build.sh core
bin/cli.sh scratch/synthetic.json > synthetic-func-dependencies-2-funcs_global-ratio-10_1-thread-1M.txt

git checkout firas/scratch
./build.sh core
bin/cli.sh scratch/synthetic.json > synthetic-master-2-funcs-copy_global-ratio-10_1-thread-1M.txt

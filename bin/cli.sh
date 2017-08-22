#! /usr/bin/env bash

if [ "$#" -ne 1 ]; then
    echo "One argument required: [conf.yaml file]"
    exit 1
fi

BIN=`dirname "$0"`
BASE=$BIN/../core
java -Xmx256g -cp "$BASE/config:$BASE/target/classes:$BASE/target/*" \
edu.stanford.futuredata.macrobase.cli.CliRunner "$1"

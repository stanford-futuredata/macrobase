#!/usr/bin/env bash
BIN=`dirname "$0"`
BASE=$BIN/..
java -Xmx10g -cp "$BASE/config:$BASE/target/classes:$BASE/target/*" \
edu.stanford.futuredata.macrobase.runner.SimpleRunner "$@"

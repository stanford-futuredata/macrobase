#!/usr/bin/env bash
BIN=`dirname "$0"`
BASE=$BIN/../core
java -verbose:gc -Xms8g -Xmx12g -cp "$BASE/target/classes:$BASE/target/*" \
edu.stanford.futuredata.macrobase.cli.CliRunner "$@"

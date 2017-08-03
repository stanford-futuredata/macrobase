#!/usr/bin/env bash
BIN=`dirname "$0"`
BASE=$BIN/../core
java -Xmx4g -cp "$BASE/config:$BASE/target/classes:$BASE/target/*" \
edu.stanford.futuredata.macrobase.cli.CliRunner "$@"

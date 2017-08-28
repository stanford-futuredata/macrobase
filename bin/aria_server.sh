#!/usr/bin/env bash
BIN=`dirname "$0"`
BASE=$BIN/../core
java -Xmx10g -ea -cp "$BASE/config:$BASE/target/classes:$BASE/target/*" \
edu.stanford.futuredata.macrobase.contrib.aria.AriaRestServer "$@"

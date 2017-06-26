#!/usr/bin/env bash
java -Xmx6g -cp "target/classes:target/*" \
edu.stanford.futuredata.macrobase.runner.SimpleRunner "$@"

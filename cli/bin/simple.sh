#!/usr/bin/env bash
java -cp "target/classes:target/*" \
edu.stanford.futuredata.macrobase.runner.SimpleRunner "$@"
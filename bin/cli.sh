#!/bin/sh
BIN=`dirname "$0"`
BASE=$BIN/../core
java -Xmx6g  -agentpath:"C:\Program Files\YourKit Java Profiler 2017.02-b68\bin\win64\yjpagent.dll" -cp "$BASE/target/classes;$BASE/target/*" \
edu.stanford.futuredata.macrobase.cli.CliRunner "$@"

#!/usr/bin/env bash
BIN=`dirname "$0"`
BASE=$BIN/../core
java -Xmx4g -cp "$BASE/config:$BASE/target/classes:$BASE/target/*" \
-agentpath:/afs/cs.stanford.edu/u/sahaana/yjp-2017.02/bin/linux-x86-64/libyjpagent.so=onexit=snapshot,sampling \
edu.stanford.futuredata.macrobase.cli.CliRunner "$@"

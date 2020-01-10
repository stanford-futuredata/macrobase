#!/usr/bin/env bash
java -Xmx10g -Xms10g -cp target/macrobase-lib-1.0-SNAPSHOT.jar:$(cat cp.txt) edu.stanford.futuredata.macrobase.runner.TestRunner $@

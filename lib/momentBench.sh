#!/usr/bin/env bash
java -Xmx8g -Xms8g -cp target/macrobase-lib-0.2.1-SNAPSHOT.jar:$(cat cp.txt) edu.stanford.futuredata.macrobase.APLMomentSummarizerBench $@
#!/usr/bin/env bash
java -Xmx32g -Xms32g -cp target/macrobase-lib-1.0-SNAPSHOT.jar:$(cat cp.txt) SamplingBench $@

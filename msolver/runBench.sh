#!/usr/bin/env bash
java -Xmx10g -Xms10g -cp target/msolver-1.0-SNAPSHOT.jar:$(cat cp.txt) \
msolver.runner.BenchRunner $@
#!/usr/bin/env bash
java -Xmx64g -Xms64g -cp target/macrobase-lib-1.0-SNAPSHOT.jar:$(cat cp.txt) InjectionBench $@

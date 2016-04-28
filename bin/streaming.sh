#!/bin/sh

java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase pipeline conf/streaming.yaml

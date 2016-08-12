#!/bin/sh

java ${JAVA_OPTS} -cp "assembly/target/*:core/target/*:frontend/target/*:contrib/target/*" macrobase.MacroBase pipeline conf/streaming.yaml

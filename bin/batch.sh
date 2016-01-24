#!/bin/sh

set -x

java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase batch conf/batch.yaml

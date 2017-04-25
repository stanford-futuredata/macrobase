#!/bin/sh
conf_file=${1:-"conf/batch.yaml"}

set -x
java ${JAVA_OPTS} -cp \
"core/target/classes:frontend/target/classes:frontend/src/main/resources/:contrib/target/classes:assembly/target/*:$CLASSPATH" \
macrobase.MacroBase pipeline $conf_file

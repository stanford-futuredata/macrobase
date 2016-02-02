#!/bin/sh
conf_file=${1:-"conf/macrobase.yaml"}

set -x

java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase server $conf_file

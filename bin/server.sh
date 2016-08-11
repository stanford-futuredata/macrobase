#!/bin/sh
conf_file=${1:-"conf/macrobase.yaml"}

set -x

java ${JAVA_OPTS} -cp "assembly/target/*:core/target/*:frontend/target/*:tools/target/*" macrobase.runtime.MacroBaseServer server $conf_file

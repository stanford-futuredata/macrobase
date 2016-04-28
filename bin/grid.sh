#!/bin/sh
conf_file=${1:-"conf/batch.yaml"}

set -x

java -Dmacrobase.pipeline.class=GridDumpPipeline ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase pipeline $conf_file

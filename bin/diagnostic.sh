#!/bin/sh
conf_file=${2:-"conf/batch.yaml"}

java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*:target/test-classes" macrobase.diagnostic.DiagnosticMain $1 $conf_file

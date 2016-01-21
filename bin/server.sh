conf_file=${1:-"conf/macrobase.yaml"}

java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase server $conf_file

ulimit -u unlimited
java -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase streaming config/streaming.yaml

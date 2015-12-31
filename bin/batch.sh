ulimit -u unlimited
java -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase batch conf/batch.yaml

# on mac, 'find /Library/Java -name jmc',
# e.g., /Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home/bin/jmc
java -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase streaming config/streaming.yaml

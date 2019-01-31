
set BIN=%cd%
set BASE=%BIN%\core
java -ea -cp "%BASE%\config;%BASE%\target\classes;%BASE%\target\*" edu.stanford.futuredata.macrobase.rest.RestServer "$@"
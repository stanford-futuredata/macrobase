package macrobase.ingest;

import macrobase.datamodel.DataFrame;
import macrobase.datamodel.Schema;

import java.util.Map;

public interface DataFrameCSVLoader {
    DataFrameCSVLoader setColumnTypes(Map<String, Schema.ColType> types);

    DataFrame load() throws Exception;
}

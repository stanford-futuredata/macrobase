package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.util.Map;

public interface DataFrameLoader {
    DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types);
    DataFrame load() throws Exception;
}

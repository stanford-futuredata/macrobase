package edu.stanford.futuredata.macrobase.contrib.aria;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;

import java.util.Map;

public class CubeAPILoader implements DataFrameLoader {
    private String uri;
    private String apiKey;

    public CubeAPILoader(
            String uri,
            String apiKey
    ) {
        this.uri = uri;
        this.apiKey = apiKey;
    }

    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        return null;
    }
}

package edu.stanford.futuredata.macrobase.contrib.aria.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CubeDimensionParser {
    private APIDimensionsResult apiResult;

    public CubeDimensionParser(APIDimensionsResult apiResult) {
        this.apiResult = apiResult;
    }
    public static CubeDimensionParser loadFromString(String rawApiString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        APIDimensionsResult apiResult = mapper.readValue(rawApiString, APIDimensionsResult.class);
        return new CubeDimensionParser(apiResult);
    }
    public static CubeDimensionParser loadFromFile(File f) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        APIDimensionsResult apiResult = mapper.readValue(f, APIDimensionsResult.class);
        return new CubeDimensionParser(apiResult);
    }


    public Map<String, List<String>> getDimensionValues() {
        Map<String, List<String>> valueMap = new HashMap<>();
        for (String dimensionName : this.apiResult.dimensions.keySet()) {
            APIDimensionValuesResult vals = this.apiResult.dimensions.get(dimensionName);
            valueMap.put(dimensionName, vals.values);
        }
        return valueMap;
    }
}

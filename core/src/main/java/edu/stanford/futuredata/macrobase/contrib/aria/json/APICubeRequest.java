package edu.stanford.futuredata.macrobase.contrib.aria.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class APICubeRequest {
    public List<APICubeRequestFilter> filters;
    public List<String> segments = new ArrayList<>();
    public String interval;
    public String granularity = "ALL";
    public List<String> operations;

    public APICubeRequest(
            List<APICubeRequestFilter> filters,
            String startTime,
            String endTime,
            List<String> operations
    ) {
        this.filters = filters;
        this.interval = String.format("%s/%s", startTime, endTime);
        this.operations = operations;
    }

    public static APICubeRequest fromDimensionValues(
            Map<String, List<String>> dimValues,
            String startTime,
            String endTime,
            List<String> operations
    ) {
        List<APICubeRequestFilter> filters = new ArrayList<>();
        for (String dimName : dimValues.keySet()) {
            APICubeRequestFilter curFilter = new APICubeRequestFilter(
                    dimName,
                    dimValues.get(dimName)
            );
            filters.add(curFilter);
        }
        return new APICubeRequest(
                filters,
                startTime,
                endTime,
                operations
        );
    }
}

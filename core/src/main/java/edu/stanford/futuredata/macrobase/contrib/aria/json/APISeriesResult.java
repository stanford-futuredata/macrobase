package edu.stanford.futuredata.macrobase.contrib.aria.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class APISeriesResult {
    public String operation;
    public List<Double> values;
    public String segment;
    public List<APIFilterResult> filters;
}

package edu.stanford.futuredata.macrobase.contrib.aria.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class APIFilterResult {
    public String dimension;
    public String operation;
    public List<String> values;
    public String action;
}

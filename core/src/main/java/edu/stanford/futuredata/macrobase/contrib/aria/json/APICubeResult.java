package edu.stanford.futuredata.macrobase.contrib.aria.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class APICubeResult {
    public String interval;
    public String granularity;
    public String units;
    public List<APISeriesResult> series;
}

package edu.stanford.futuredata.macrobase.contrib.aria.json;

import java.util.List;

public class APICubeRequestFilter {
    public String dimension;
    public String operation = "In";
    public List<String> values;
    public String action = "Separate";

    public APICubeRequestFilter(String d, List<String> v) {
        this.dimension = d;
        this.values = v;
    }
}

package macrobase.ingest;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import java.util.HashMap;

public class DatumEncoder {
    private HashMap<Integer, String> attributeDimensionNameMap = Maps.newHashMap();
    private HashMap<Integer, String> metricDimensionNameMap = Maps.newHashMap();
    private HashMap<Integer, HashBiMap<String, Integer>> integerEncoding = new HashMap<>();

    private Integer nextKey = 0;

    public void recordAttributeName(int dimension, String attribute) {
        attributeDimensionNameMap.put(dimension, attribute);
    }

    public void recordMetricName(int dimension, String metric) {
        metricDimensionNameMap.put(dimension, metric);
    }

    public String getAttributeName(int dimension) {
        return attributeDimensionNameMap.get(dimension);
    }

    public String getMetricName(int dimension) {
        return metricDimensionNameMap.get(dimension);
    }

    public int getIntegerEncoding(int dimension, String attr) {
        integerEncoding.computeIfAbsent(dimension, key -> HashBiMap.<String, Integer>create());

        BiMap<String, Integer> dimensionMap = integerEncoding.get(dimension);
        Integer ret = dimensionMap.get(attr);
        if(ret == null) {
            ret = nextKey;
            dimensionMap.put(attr, ret);
            nextKey++;
        }

        return ret;
    }
}

package macrobase.ingest;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import macrobase.ingest.result.ColumnValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DatumEncoder {
    private HashMap<Integer, String> attributeDimensionNameMap = Maps.newHashMap();
    private HashMap<Integer, HashBiMap<String, Integer>> integerEncoding = new HashMap<>();
    private HashMap<Integer, Integer> integerToColumn = new HashMap();

    private Integer nextKey = 0;

    public void recordAttributeName(int dimension, String attribute) {
        attributeDimensionNameMap.put(dimension, attribute);
    }

    public ColumnValue getAttribute(int encodedAttr) {
        int matchingColumn = integerToColumn.get(encodedAttr);

        String columnName = attributeDimensionNameMap.get(matchingColumn);
        String columnValue = integerEncoding.get(matchingColumn).inverse().get(encodedAttr);

        return new ColumnValue(columnName, columnValue);
    }

    public List<ColumnValue> getColsFromAttrSet(Set<Integer> attrs) {
        List<ColumnValue> ret = new ArrayList<>();
        for(Integer item : attrs) {
            ret.add(getAttribute(item));
        }

        return ret;
    }

    public int getIntegerEncoding(int dimension, String attr) {
        integerEncoding.computeIfAbsent(dimension, key -> HashBiMap.<String, Integer>create());


        BiMap<String, Integer> dimensionMap = integerEncoding.get(dimension);
        Integer ret = dimensionMap.get(attr);
        if(ret == null) {
            ret = nextKey;
            integerToColumn.put(nextKey, dimension);
            dimensionMap.put(attr, ret);
            nextKey++;
        }

        return ret;
    }
}

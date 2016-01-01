package macrobase.ingest;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import macrobase.ingest.result.ColumnValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatumEncoder {
    private HashMap<Integer, String> attributeDimensionNameMap = new HashMap<>();
    private HashMap<Integer, HashBiMap<String, Integer>> integerEncoding = new HashMap<>();
    private HashMap<Integer, Integer> integerToColumn = new HashMap<>();

    private Integer nextKey = 0;

    public void updateAttributeDimensions(Map<Integer, Integer> oldToNewRemapping) {
        HashMap<Integer, String> newAttributeDimensionNameMap = new HashMap<>();
        HashMap<Integer, HashBiMap<String, Integer>> newIntegerEncoding = new HashMap<>();
        HashMap<Integer, Integer> newIntegerToColumn = new HashMap<>();

        for(Map.Entry<Integer, String> entry : attributeDimensionNameMap.entrySet()) {
            int dim = entry.getKey();
            if(oldToNewRemapping.containsKey(dim)) {
                dim = oldToNewRemapping.get(dim);
            }
            newAttributeDimensionNameMap.put(dim, entry.getValue());
        }

        for(Map.Entry<Integer, HashBiMap<String, Integer>> entry :
                integerEncoding.entrySet()) {
            int dim = entry.getKey();
            if(oldToNewRemapping.containsKey(dim)) {
                dim = oldToNewRemapping.get(dim);
            }
            newIntegerEncoding.put(dim, entry.getValue());
        }

        for(Map.Entry<Integer, Integer> entry :
                integerToColumn.entrySet()) {
            int dim = entry.getKey();
            if(oldToNewRemapping.containsKey(dim)) {
                dim = oldToNewRemapping.get(dim);
            }
            newIntegerToColumn.put(dim, entry.getValue());
        }

        attributeDimensionNameMap = newAttributeDimensionNameMap;
        integerEncoding = newIntegerEncoding;
        integerToColumn = newIntegerToColumn;
    }

    // kind of a hack...
    public void copy(DatumEncoder other) {
        attributeDimensionNameMap.clear();
        for(Map.Entry<Integer, String> entry : other.attributeDimensionNameMap.entrySet()) {
            attributeDimensionNameMap.put(entry.getKey(), entry.getValue());
        }

        integerEncoding.clear();
        for(Map.Entry<Integer, HashBiMap<String, Integer>> entry :
                other.integerEncoding.entrySet()) {
            integerEncoding.put(entry.getKey(), entry.getValue());
        }

        integerToColumn.clear();
        for(Map.Entry<Integer, Integer> entry :
                other.integerToColumn.entrySet()) {
            integerToColumn.put(entry.getKey(), entry.getValue());
        }

        nextKey = other.nextKey;
    }

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

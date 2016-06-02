package macrobase.ingest;

import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatumEncoder {
    private static final Logger log = LoggerFactory.getLogger(DatumEncoder.class);

    private HashMap<Integer, String> attributeDimensionNameMap = new HashMap<>();
    private HashMap<Integer, Map<String, Integer>> integerEncoding = new HashMap<>();
    private HashMap<Integer, Integer> integerToColumn = new HashMap<>();

    private Integer nextKey = 0;

    // kryo
    public DatumEncoder() {}

    // kind of a hack...
    public void copy(DatumEncoder other) {
        attributeDimensionNameMap.clear();
        for (Map.Entry<Integer, String> entry : other.attributeDimensionNameMap.entrySet()) {
            attributeDimensionNameMap.put(entry.getKey(), entry.getValue());
        }

        integerEncoding.clear();
        for (Map.Entry<Integer, Map<String, Integer>> entry :
                other.integerEncoding.entrySet()) {
            integerEncoding.put(entry.getKey(), entry.getValue());
        }

        integerToColumn.clear();
        for (Map.Entry<Integer, Integer> entry :
                other.integerToColumn.entrySet()) {
            integerToColumn.put(entry.getKey(), entry.getValue());
        }

        nextKey = other.nextKey;
    }

    public void recordAttributeName(int dimension, String attribute) {
        attributeDimensionNameMap.put(dimension, attribute);
    }

    public String getAttributeName(Integer dimension) {
        if (dimension != null)
            return attributeDimensionNameMap.get(dimension);
        return null;
    }

    public ColumnValue getAttribute(int encodedAttr) {
        int matchingColumn = integerToColumn.get(encodedAttr);

        String columnName = attributeDimensionNameMap.get(matchingColumn);
        Map<String, Integer> columnEncoding = integerEncoding.get(matchingColumn);
        String columnValue = null;
        for (Map.Entry<String, Integer> ce : columnEncoding.entrySet()) {
            if (ce.getValue() == encodedAttr) {
                columnValue = ce.getKey();
            }
        }

        return new ColumnValue(columnName, columnValue);
    }

    public List<ColumnValue> getColsFromAttrSet(Set<Integer> attrs) {
        List<ColumnValue> ret = new ArrayList<>();
        for (Integer item : attrs) {
            ret.add(getAttribute(item));
        }

        return ret;
    }


    public List<ColumnValue> getColsFromAttr(Integer item) {
        List<ColumnValue> ret = new ArrayList<>();
        ret.add(getAttribute(item));

        return ret;
    }

    public int getIntegerEncoding(int dimension, String attr) {
        integerEncoding.computeIfAbsent(dimension, key -> new HashMap<>());


        Map<String, Integer> dimensionMap = integerEncoding.get(dimension);
        Integer ret = dimensionMap.get(attr);
        if (ret == null) {
            ret = nextKey;
            integerToColumn.put(nextKey, dimension);
            dimensionMap.put(attr, ret);
            nextKey++;
        }

        return ret;
    }
    
    public int getNextKey(){
    	return nextKey;
    }
}

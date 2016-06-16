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
    private HashMap<Integer, Map<Integer, String>> stringEncoding = new HashMap<>();

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

        stringEncoding.clear();
        for (Map.Entry<Integer, Map<Integer, String>> entry :
                other.stringEncoding.entrySet()) {
            stringEncoding.put(entry.getKey(), entry.getValue());
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
        String columnValue = stringEncoding.get(matchingColumn).get(encodedAttr);

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
        stringEncoding.computeIfAbsent(dimension, key -> new HashMap<>());

        Map<String, Integer> integerMapDimensionMap = integerEncoding.get(dimension);
        Map<Integer, String> stringDimensionMap = stringEncoding.get(dimension);

        Integer ret = integerMapDimensionMap.get(attr);
        if (ret == null) {
            ret = nextKey;
            integerToColumn.put(nextKey, dimension);
            integerMapDimensionMap.put(attr, ret);
            stringDimensionMap.put(ret, attr);
            nextKey++;
        }

        return ret;
    }
    
    public int getNextKey(){
    	return nextKey;
    }
}

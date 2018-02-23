package edu.stanford.futuredata.macrobase.distributed.datamodel;

import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistributedDataFrame {
    public Schema schema;
    public JavaPairRDD<String[], double[]> dataFrameRDD;
    private Map<String, Integer> nameToIndexMap;

    public DistributedDataFrame(Schema schema, JavaPairRDD<String[], double[]> dataFrameRDD) {
        this.schema = schema;
        this.dataFrameRDD = dataFrameRDD;

        nameToIndexMap = new HashMap<>();
        List<String> doubleNames = schema.getColumnNamesByType(Schema.ColType.DOUBLE);
        List<String> stringNames = schema.getColumnNamesByType(Schema.ColType.STRING);
        for (int i = 0; i < doubleNames.size(); i++)
            nameToIndexMap.put(doubleNames.get(i), i);
        for (int i = 0; i < stringNames.size(); i++)
            nameToIndexMap.put(stringNames.get(i), i);
    }

    public Schema.ColType getTypeOfColumn(String colName) {
        return schema.getColumnTypeByName(colName);
    }

    public int getIndexOfColumn(String colName) {
        return nameToIndexMap.get(colName);
    }

    public void addColumnToSchema(String colName, Schema.ColType colType) {
        schema.addColumn(colType, colName);
        int index  = schema.getColumnNamesByType(colType).size() - 1;
        nameToIndexMap.put(colName, index);
    }
}

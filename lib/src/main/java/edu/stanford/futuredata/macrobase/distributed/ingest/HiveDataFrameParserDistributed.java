package edu.stanford.futuredata.macrobase.distributed.ingest;

import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class HiveDataFrameParserDistributed implements Serializable {

  private Logger log = LoggerFactory.getLogger(HiveDataFrameParserDistributed.class);
  private final List<String> requiredColumns;
  private Map<String, ColType> columnTypes;
  private final String query;

  public HiveDataFrameParserDistributed(String query, List<String> requiredColumns) {
    this.requiredColumns = requiredColumns;
    this.query = query;
  }

  public void setColumnTypes(Map<String, Schema.ColType> types) {
    this.columnTypes = types;
  }

  public DistributedDataFrame load(SparkSession spark, int numPartitions) {

    Dataset<Row> hiveDf = spark.sql(query).repartition(numPartitions);
    final String[] header = hiveDf.columns();

    // Define a schema from the header
    int numColumns = header.length;
    int schemaLength = requiredColumns.size();
    int schemaIndexMap[] = new int[numColumns];
    Arrays.fill(schemaIndexMap, -1);

    String[] columnNameList = new String[schemaLength];
    Schema.ColType[] columnTypeList = new Schema.ColType[schemaLength];
    for (int c = 0, schemaIndex = 0; c < numColumns; c++) {
      String columnName = header[c];
      Schema.ColType t = columnTypes.getOrDefault(columnName, Schema.ColType.STRING);
      if (requiredColumns.contains(columnName)) {
        columnNameList[schemaIndex] = columnName;
        columnTypeList[schemaIndex] = t;
        schemaIndexMap[c] = schemaIndex;
        schemaIndex++;
      }
    }

    // Make sure to generate the schema in the right order
    Schema schema = new Schema();
    int numStringColumns = 0;
    int numDoubleColumns = 0;
    for (int c = 0; c < schemaLength; c++) {
      schema.addColumn(columnTypeList[c], columnNameList[c]);
      if (columnTypeList[c] == Schema.ColType.STRING) {
        numStringColumns++;
      } else if (columnTypeList[c] == Schema.ColType.DOUBLE) {
        numDoubleColumns++;
      } else {
        throw new RuntimeException("Bad ColType");
      }
    }

    final int numStringColumnsFinal = numStringColumns;
    final int numDoubleColumnsFinal = numDoubleColumns;

    hiveDf.cache();
    JavaPairRDD<String[], double[]> distributedDataFrame = hiveDf.toJavaRDD().mapPartitionsToPair(
        (Iterator<Row> iter) -> {
          List<Tuple2<String[], double[]>> parsedRows = new ArrayList<>();
          while(iter.hasNext()) {
            final Row row = iter.next();
            String[] stringRow = new String[numStringColumnsFinal];
            double[] doubleRow = new double[numDoubleColumnsFinal];
            boolean goodRow = true;
            for (int c = 0, stringColNum = 0, doubleColNum = 0; c < numColumns; c++) {
              if (schemaIndexMap[c] >= 0) {
                int schemaIndex = schemaIndexMap[c];
                Schema.ColType t = columnTypeList[schemaIndex];
                try {
                  String rowValue = row.getString(c);
                  if (t == Schema.ColType.STRING) {
                    stringRow[stringColNum++] = rowValue;
                  } else if (t == Schema.ColType.DOUBLE) {
                    try {
                      doubleRow[doubleColNum] = Double.parseDouble(rowValue);
                    } catch (NumberFormatException e) {
                      doubleRow[doubleColNum] = Double.NaN;
                    }
                    doubleColNum++;
                  } else {
                    throw new RuntimeException("Bad ColType");
                  }
                } catch (ArrayIndexOutOfBoundsException e) {
                  goodRow = false;
                  break;
                }
              }
            }
            if (goodRow) {
              parsedRows.add(new Tuple2<>(stringRow, doubleRow));
            }
          }
          return parsedRows.iterator();
        }, true
    );

    return new DistributedDataFrame(schema, distributedDataFrame);
  }
}

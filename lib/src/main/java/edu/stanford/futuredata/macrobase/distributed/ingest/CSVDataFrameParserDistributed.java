package edu.stanford.futuredata.macrobase.distributed.ingest;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.*;

public class CSVDataFrameParserDistributed implements Serializable{
    private Logger log = LoggerFactory.getLogger(edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser.class);
    private final List<String> requiredColumns;
    private Map<String, Schema.ColType> columnTypes;
    private String fileName;

    public CSVDataFrameParserDistributed(String fileName, List<String> requiredColumns) throws IOException {
        this.requiredColumns = requiredColumns;
        this.fileName = fileName;
    }

    public void setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
    }

    public DistributedDataFrame load(JavaSparkContext sparkContext, int numPartitions) throws Exception {
        // Increase the number of partitions to ensure enough memory for parsing overhead
        int increasedPartitions = numPartitions * 10;
        // For unit tests
        if (numPartitions == 0)
            increasedPartitions = 1;
        JavaRDD<String> fileRDD = sparkContext.textFile(fileName, increasedPartitions);
        // Extract the header
        CsvParserSettings headerSettings = new CsvParserSettings();
        headerSettings.getFormat().setLineSeparator("\n");
        headerSettings.setMaxCharsPerColumn(16384);
        CsvParser headerParser = new CsvParser(headerSettings);
        String[] header = headerParser.parseLine(fileRDD.first());
        // Remove the header
        fileRDD = fileRDD.mapPartitionsWithIndex(
                (Integer index, Iterator<String> iter) -> {
                    if (index == 0) {
                        iter.next();
                    }
                    return iter;
                }, true
        );
        JavaRDD<String> repartitionedRDD = fileRDD.repartition(increasedPartitions);

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

        // Distribute and parse
        repartitionedRDD.cache();
        JavaPairRDD<String[], double[]> distributedDataFrame = repartitionedRDD.mapPartitionsToPair(
                (Iterator<String> iter) -> {
                    CsvParserSettings settings = new CsvParserSettings();
                    settings.getFormat().setLineSeparator("\n");
                    settings.setMaxCharsPerColumn(16384);
                    CsvParser csvParser = new CsvParser(settings);
                    List<Tuple2<String[], double[]>> parsedRows = new ArrayList<>();
                    while(iter.hasNext()) {
                        String row = iter.next();
                        String[] parsedRow = csvParser.parseLine(row);
                        String[] stringRow = new String[numStringColumnsFinal];
                        double[] doubleRow = new double[numDoubleColumnsFinal];
                        boolean goodRow = true;
                        for (int c = 0, stringColNum = 0, doubleColNum = 0; c < numColumns; c++) {
                            if (schemaIndexMap[c] >= 0) {
                                int schemaIndex = schemaIndexMap[c];
                                Schema.ColType t = columnTypeList[schemaIndex];
                                try {
                                    String rowValue = parsedRow[c];
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

        DistributedDataFrame df = new DistributedDataFrame(schema, distributedDataFrame);
        return df;
    }
}


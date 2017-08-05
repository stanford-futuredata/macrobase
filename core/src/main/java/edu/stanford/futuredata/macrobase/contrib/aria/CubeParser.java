package edu.stanford.futuredata.macrobase.contrib.aria;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Parse aria json cube result into a dataframe
 */
public class CubeParser {
    private APICubeResult apiResult;

    private ArrayList<String> attributeColumns;
    private ArrayList<String> aggregateColumns;

    public CubeParser(APICubeResult result) {
        this.apiResult = result;
    }
    public static CubeParser loadFromFile(File f) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        APICubeResult cube = mapper.readValue(f, APICubeResult.class);
        return new CubeParser(cube);
    }

    public DataFrame getDataFrame() {
        if (apiResult.series.isEmpty()) {
            return null;
        }
        APISeriesResult firstSeries = apiResult.series.get(0);
        attributeColumns = new ArrayList<>();
        for (APIFilterResult filter : firstSeries.filters) {
            attributeColumns.add(filter.dimension);
        }

        // attribute combination -> time series of [aggregate -> value]
        Map<List<String>, List<Map<String, Double>>> values = new HashMap<>();

        // Infer the set of aggregates from the set of all operations we see
        Set<String> operations = new HashSet<>();
        for (APISeriesResult series : apiResult.series) {
            operations.add(series.operation);
            List<String> attributeValues = new ArrayList<>();
            for (APIFilterResult filter : series.filters) {
                attributeValues.add(filter.values.get(0));
            }
            // Special case logic to populate lists and maps the first time we access them
            if (!values.containsKey(attributeValues)) {
                ArrayList<Map<String, Double>> cubeCell = new ArrayList<>();
                for (Double d : series.values) {
                    HashMap<String, Double> aggValue = new HashMap<>();
                    aggValue.put(series.operation, d);
                    cubeCell.add(aggValue);
                }
                values.put(attributeValues, cubeCell);
            } else {
                List<Map<String, Double>> cubeCell = values.get(attributeValues);
                for (int i = 0; i < series.values.size(); i++) {
                    cubeCell.get(i).put(series.operation, series.values.get(i));
                }
            }
        }
        aggregateColumns = new ArrayList<>(operations);

        Schema s = new Schema();
        for (String attributeColumn : attributeColumns) {
            s.addColumn(Schema.ColType.STRING, attributeColumn);
        }
        // Each cube cell has a time series of values for each aggregate
        s.addColumn(Schema.ColType.STRING, "series_index");
        for (String aggregate : aggregateColumns) {
            s.addColumn(Schema.ColType.DOUBLE, aggregate);
        }
        List<Row> rows = new ArrayList<>();
        for (List<String> attributeValues : values.keySet()) {
            List<Map<String, Double>> aggValueSeries = values.get(attributeValues);
            for (int i = 0 ; i < aggValueSeries.size(); i++){
                Map<String, Double> aggValues = aggValueSeries.get(i);
                List<Object> rawRow = new ArrayList<>();
                rawRow.addAll(attributeValues);
                rawRow.add(String.valueOf(i));
                for (String aggregate : aggregateColumns) {
                    rawRow.add(aggValues.get(aggregate));
                }
                rows.add(new Row(rawRow));
            }
        }

        DataFrame df = new DataFrame(s, rows);
        return df;
    }

    public ArrayList<String> getAttributeColumns() {
        return attributeColumns;
    }
}

package edu.stanford.futuredata.macrobase.analysis.transform;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.*;

public class MetricBucketTransformer implements Transformer {
    private String columnSuffix = "_a";
    private double[] boundaryPercentiles = {10.0, 90.0};
    private boolean useSimpleNames = false;

    private List<String> metricColumns;
    private List<String> transformedColumnNames;

    private DataFrame transformedDF;

    public MetricBucketTransformer(List<String> columns) {
        this.metricColumns = columns;
        int d = columns.size();
        transformedColumnNames = new ArrayList<>(d);
        for (String colName : metricColumns) {
            transformedColumnNames.add(colName+columnSuffix);
        }
    }
    public MetricBucketTransformer(String column) {
        this(Collections.singletonList(column));
    }

    @Override
    public void process(DataFrame input) throws Exception {
        transformedDF = input.copy();

        int d = metricColumns.size();
        for (int colIdx = 0; colIdx < d; colIdx++) {
            String colName = metricColumns.get(colIdx);
            double[] colValues = input.getDoubleColumnByName(colName);

            int n = colValues.length;
            int k = boundaryPercentiles.length;
            double[] curBoundaries = new double[k];
            Percentile pCalc = new Percentile();
            pCalc.setData(colValues);
            for (int i = 0; i < k; i++) {
                curBoundaries[i] = pCalc.evaluate(boundaryPercentiles[i]);
            }

            String[] bucketNames = new String[k+1];
            if (useSimpleNames) {
                for (int i = 0; i < k+1; i++) {
                    bucketNames[i] = String.format("%s:%d", colName, i);
                }
            } else {
                bucketNames[0] = String.format("%s:[,%g]", colName, curBoundaries[0]);
                for (int i = 1; i < k; i++) {
                    bucketNames[i] = String.format("%s:[%g,%g]", colName, curBoundaries[i - 1], curBoundaries[i]);
                }
                bucketNames[k] = String.format("%s:[%g,]", colName, curBoundaries[k - 1]);
            }

            String[] transformedColValues = new String[n];
            for (int i = 0; i < n; i++) {
                int searchIdx = Arrays.binarySearch(curBoundaries, colValues[i]);
                if (searchIdx < 0) {
                    searchIdx = -searchIdx - 1;
                }
                transformedColValues[i] = bucketNames[searchIdx];
            }
            transformedDF.addStringColumn(
                    transformedColumnNames.get(colIdx),
                    transformedColValues
            );
        }
    }

    @Override
    public DataFrame getResults() {
        return transformedDF;
    }

    public String getColumnSuffix() {
        return columnSuffix;
    }

    public void setColumnSuffix(String columnSuffix) {
        this.columnSuffix = columnSuffix;
    }

    public double[] getBoundaryPercentiles() {
        return boundaryPercentiles;
    }

    public void setBoundaryPercentiles(double[] boundaryPercentiles) {
        this.boundaryPercentiles = boundaryPercentiles;
    }

    public boolean isUseSimpleNames() {
        return useSimpleNames;
    }

    public void setUseSimpleNames(boolean useSimpleNames) {
        this.useSimpleNames = useSimpleNames;
    }

    public List<String> getTransformedColumnNames() {
        return transformedColumnNames;
    }
}

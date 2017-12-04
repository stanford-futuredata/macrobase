package edu.stanford.futuredata.macrobase.analysis.transform;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.*;

/**
 * Transform real valued columns into categorical string columns.
 * Transformed columns are added to a copy of the dataframe and appended with
 * a suffix to distinguish them from the original column.
 * Useful for using correlated metrics as explanatory values.
 * By default transforms columns by bucketing them into low-med-high values
 * based on percentile.
 */
public class MetricBucketTransformer implements Transformer {
    // Transformed columns are added to the dataframe under a suffix.
    private String columnSuffix = "_a";
    // Boundaries of the buckets used for classification. n boundaries -> n+1 buckets
    private double[] boundaryPercentiles = {10.0, 90.0};
    // The strings used to encode which bucket a value falls in can be either a simple index
    // or an encoding of the range of the bucket.
    private boolean simpleBucketValues = false;

    private List<String> metricColumns;
    private List<String> transformedColumnNames;

    private DataFrame transformedDF;

    /**
     * @param columns set of columns to transform
     */
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
            if (simpleBucketValues) {
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
            transformedDF.addColumn(
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

    public boolean isSimpleBucketValues() {
        return simpleBucketValues;
    }

    public void setSimpleBucketValues(boolean simpleBucketValues) {
        this.simpleBucketValues = simpleBucketValues;
    }

    public List<String> getTransformedColumnNames() {
        return transformedColumnNames;
    }
}

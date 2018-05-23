package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class APLSuciuSummarizer extends APLSummarizer {

    private Logger log = LoggerFactory.getLogger("APLInterventionSummarizer");
    private boolean useBitmaps;

    public APLSuciuSummarizer(boolean useBitmaps) {
        this.useBitmaps = useBitmaps;
    }

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("OutlierCount");
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        AggregationOp[] curOps = {AggregationOp.SUM};
        return curOps;
    }

    @Override
    public int[][] getEncoded(List<String[]> columns, DataFrame input) {
        return encoder.encodeAttributesWithSupport(columns, minOutlierSupport,
                input.getDoubleColumnByName(outlierColumn), useBitmaps);
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);

        double[][] aggregateColumns = new double[1][];
        aggregateColumns[0] = outlierCol;

        return aggregateColumns;
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new SupportQualityMetric(0)
        );
        qualityMetricList.add(
                new InterventionQualityMetric(0)
        );
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Arrays.asList(minOutlierSupport, 0.0);
    }

    @Override
    public double getNumberOutliers(double[][] aggregates) {
        double count = 0.0;
        double[] outlierCount = aggregates[0];
        for (int i = 0; i < outlierCount.length; i++) {
            count += outlierCount[i];
        }
        return count;
    }
}

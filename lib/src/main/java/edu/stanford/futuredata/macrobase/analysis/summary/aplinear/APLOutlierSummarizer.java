package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class APLOutlierSummarizer extends BatchSummarizer{
    Logger log = LoggerFactory.getLogger("APLOutlierSummarizer");

    private String countColumn = "Count";
    private APrioriLinear aplKernel;

    private double minRatioMetric = 2.0;

    int numRows;
    AttributeEncoder encoder;
    List<APLExplanationResult> aplResults;

    public APLOutlierSummarizer() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new SupportMetric(0)
        );
        qualityMetricList.add(
                new GlobalRatioMetric(0, 1)
        );
        aplKernel = new APrioriLinear(
                qualityMetricList,
                Arrays.asList(minOutlierSupport, minRatioMetric)
        );
    }

    @Override
    public void process(DataFrame input) throws Exception {
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);
        numRows = outlierCol.length;
        double[] countCol = null;
        if (countColumn != null) {
            countCol = input.getDoubleColumnByName(countColumn);
        } else {
            countCol = new double[numRows];
            for (int i = 0; i < numRows; i++) {
                countCol[i] = 1.0;
            }
        }

        encoder = new AttributeEncoder();
        encoder.setColumnNames(attributes);
        long startTime = System.currentTimeMillis();
        List<int[]> encoded = encoder.encodeAttributes(
                input.getStringColsByName(attributes)
        );
        long elapsed = System.currentTimeMillis() - startTime;
        log.debug("Encoded in: {}", elapsed);
        log.debug("Encoded Categories: {}", encoder.getNextKey());

        double[][] aggregateColumns = new double[2][];
        aggregateColumns[0] = outlierCol;
        aggregateColumns[1] = countCol;
        aplResults = aplKernel.explain(encoded, aggregateColumns);
        System.out.println(aplResults);
    }

    @Override
    public Explanation getResults() {
        return null;
    }

    public String getCountColumn() {
        return countColumn;
    }

    public void setCountColumn(String countColumn) {
        this.countColumn = countColumn;
    }

    public double getMinRatioMetric() {
        return minRatioMetric;
    }

    public void setMinRatioMetric(double minRatioMetric) {
        this.minRatioMetric = minRatioMetric;
    }
}

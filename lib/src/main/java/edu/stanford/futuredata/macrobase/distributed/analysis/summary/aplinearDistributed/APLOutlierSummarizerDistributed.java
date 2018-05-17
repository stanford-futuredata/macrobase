package edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Summarizer that works over both cube and row-based labeled ratio-based
 * outlier summarization.
 */
public class APLOutlierSummarizerDistributed extends APLSummarizerDistributed {
    private Logger log = LoggerFactory.getLogger("APLOutlierSummarizerDistributed");
    private boolean useBitMaps;

    public APLOutlierSummarizerDistributed(boolean useBitMaps) {this.useBitMaps = useBitMaps;}

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("Outliers", "Count");
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        AggregationOp[] curOps = {AggregationOp.SUM, AggregationOp.SUM};
        return curOps;
    }

    @Override
    public JavaPairRDD<int[], double[]> getEncoded(
            JavaPairRDD<String[], double[]> partitionedDataFrame) {
        return encoder.encodeAttributesWithSupport(partitionedDataFrame,
                attributes.size(),
                minOutlierSupport,
                0, useBitMaps);
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new SupportQualityMetric(0)
        );
        switch (ratioMetric) {
            case "risk_ratio":
            case "riskratio":
                qualityMetricList.add(
                        new RiskRatioQualityMetric(0, 1));
                break;
            case "global_ratio":
            case "globalratio":
            default:
                qualityMetricList.add(
                        new GlobalRatioQualityMetric(0, 1));
        }
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Arrays.asList(minOutlierSupport, minRatioMetric);
    }

    public double getMinRatioMetric() {
        return minRatioMetric;
    }
}

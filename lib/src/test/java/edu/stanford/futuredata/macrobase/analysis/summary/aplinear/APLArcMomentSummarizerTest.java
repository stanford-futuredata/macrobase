package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.classify.QuantileClassifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class APLArcMomentSummarizerTest {
    DataFrame getTestCube() throws Exception {
        List<String> metricCols = Arrays.asList(
                "latency:min",
                "latency:max",
                "latency:p05",
                "latency:p10",
                "latency:p50",
                "latency:p90",
                "latency:p95",
                "asinh:0:sum",
                "asinh:1:sum",
                "asinh:2:sum",
                "asinh:3:sum",
                "asinh:4:sum",
                "asinh:5:sum",
                "asinh:6:sum",
                "asinh:7:sum",
                "asinh:8:sum"
        );
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("country", Schema.ColType.STRING);
        schema.put("app_version", Schema.ColType.STRING);
        schema.put("device", Schema.ColType.STRING);
        for (String m : metricCols) {
            schema.put(m, Schema.ColType.DOUBLE);
        }
        DataFrameLoader loader = new CSVDataFrameParser(
                "src/test/resources/app_latency_arc_cube.csv",
                schema
        );
        DataFrame df = loader.load();
        return df;
    }

    @Test
    public void testMoments() throws Exception{
        DataFrame df = getTestCube();

        APLArcMomentSummarizer momentSummarizer = new APLArcMomentSummarizer();
        momentSummarizer.setMinColumn("latency:min");
        momentSummarizer.setMaxColumn("latency:max");
        momentSummarizer.setPowerSumColumns(Arrays.asList(
                "asinh:0:sum",
                "asinh:1:sum",
                "asinh:2:sum",
                "asinh:3:sum",
                "asinh:4:sum",
                "asinh:5:sum",
                "asinh:6:sum",
                "asinh:7:sum",
                "asinh:8:sum"
        ));
        momentSummarizer.setK(9);
        momentSummarizer.setUseCascade(true);
        momentSummarizer.setUseGlobalRatio(true);
        momentSummarizer.setUseCascade(true);
        momentSummarizer.setMinRatioMetric(6.0);
        momentSummarizer.setMinSupport(0.1);
        momentSummarizer.setQuantileCutoff(0.95);

        List<String> explanationAttributes = Arrays.asList(
                "country",
                "app_version",
                "device"
        );
        momentSummarizer.setAttributes(explanationAttributes);
        momentSummarizer.process(df);
        APLExplanation e = momentSummarizer.getResults();
        assertEquals(1, e.getResults().size());
    }

    @Test
    public void testPercentile() throws Exception {
        DataFrame df = getTestCube();
        LinkedHashMap<String, Double> qcols = new LinkedHashMap<String, Double>() {{
            put("latency:min", 0.0);
            put("latency:p05", 0.05);
            put("latency:p10", 0.1);
            put("latency:p50", 0.5);
            put("latency:p90", 0.9);
            put("latency:p95", 0.95);
            put("latency:max", 1.0);
        }};
        QuantileClassifier qc = new QuantileClassifier(
                "asinh:0:sum",
                qcols
        );
        qc.setPercentile(0.95);
        qc.process(df);
        DataFrame dfc = qc.getResults();

        List<String> explanationAttributes = Arrays.asList(
                "country",
                "app_version",
                "device"
        );
        APLOutlierSummarizer summ = new APLOutlierSummarizer();
        summ.setCountColumn("asinh:0:sum");
        summ.setOutlierColumn(qc.getOutputColumnName());
        summ.setMinSupport(.1);
        summ.setMinRatioMetric(3.0);
        summ.setAttributes(explanationAttributes);
        summ.process(dfc);
        APLExplanation e = summ.getResults();
        assertEquals(1, e.getResults().size());
    }

}
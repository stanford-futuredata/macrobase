package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

public class APLOutlierSummarizerTest {

    @Test
    public void testSimple() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("usage", Schema.ColType.DOUBLE);
        schema.put("latency", Schema.ColType.DOUBLE);
        schema.put("location", Schema.ColType.STRING);
        schema.put("version", Schema.ColType.STRING);
        DataFrameLoader loader = new CSVDataFrameParser(
                "src/test/resources/sample.csv",
                Arrays.asList("usage", "latency", "location", "version")
        ).setColumnTypes(schema);
        DataFrame df = loader.load();

        PercentileClassifier pc = new PercentileClassifier("usage")
                .setPercentile(1.0);
        pc.process(df);
        DataFrame dfClassified = pc.getResults();

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version"
        );
        APLOutlierSummarizer summ = new APLOutlierSummarizer(true);
        summ.setMinSupport(.01);
        summ.setMinRatioMetric(10.0);
        summ.setAttributes(explanationAttributes);
        summ.process(dfClassified);

        APLExplanation e = summ.getResults();
        List<APLExplanationResult> results = e.getResults();
        assertEquals(20.0, e.numOutliers(), 1e-10);
        assertEquals(1, results.size());
    }

    @Test
    public void testCubeOrder3() throws Exception {
        DataFrame df = new DataFrame();
        String[] col1 = {"a1", "a2", "a1", "a1"};
        String[] col2 = {"b1", "b1", "b2", "b1"};
        String[] col3 = {"c1", "c1", "c1", "c2"};
        double[] counts = {100, 300, 400, 500};
        double[] oCounts = {30, 5, 5, 7};
        df.addColumn("col1", col1);
        df.addColumn("col2", col2);
        df.addColumn("col3", col3);
        df.addColumn("counts", counts);
        df.addColumn("oCounts", oCounts);

        List<String> explanationAttributes = Arrays.asList(
                "col1",
                "col2",
                "col3"
        );
        APLOutlierSummarizer summ = new APLOutlierSummarizer(false);
        summ.setCountColumn("counts");
        summ.setOutlierColumn("oCounts");
        summ.setMinSupport(.1);
        summ.setMinRatioMetric(3.0);
        summ.setAttributes(explanationAttributes);
        summ.process(df);
        APLExplanation e = summ.getResults();
        assertEquals(1, e.getResults().size());
        assertTrue(e.prettyPrint().contains("col1=a1"));
        assertEquals(47.0, e.numOutliers(), 1e-10);
    }

    @Test
    public void testSimpleOrder3() throws Exception {
        DataFrame df = new DataFrame();
        String[] col1 = {"a1", "a2", "a1", "a1"};
        String[] col2 = {"b1", "b1", "b2", "b1"};
        String[] col3 = {"c1", "c1", "c1", "c2"};
        double[] counts = {1, 1, 1, 1};
        double[] oCounts = {1, 0, 0, 0};
        df.addColumn("col1", col1);
        df.addColumn("col2", col2);
        df.addColumn("col3", col3);
        df.addColumn("counts", counts);
        df.addColumn("oCounts", oCounts);

        List<String> explanationAttributes = Arrays.asList(
                "col1",
                "col2",
                "col3"
        );
        APLOutlierSummarizer summ = new APLOutlierSummarizer(true);
        summ.setCountColumn("counts");
        summ.setOutlierColumn("oCounts");
        summ.setMinSupport(.1);
        summ.setMinRatioMetric(3.0);
        summ.setAttributes(explanationAttributes);
        summ.process(df);
        APLExplanation e = summ.getResults();
        assertEquals(1, e.getResults().size());
        assertTrue(e.prettyPrint().contains("col1=a1"));
        assertEquals(1, e.numOutliers(), 1e-10);
    }
}
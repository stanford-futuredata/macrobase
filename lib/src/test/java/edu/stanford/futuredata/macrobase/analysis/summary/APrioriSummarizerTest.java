package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class APrioriSummarizerTest {
    @Test
    public void testSimple() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("usage", Schema.ColType.DOUBLE);
        schema.put("latency", Schema.ColType.DOUBLE);
        schema.put("location", Schema.ColType.STRING);
        schema.put("version", Schema.ColType.STRING);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/sample.csv"
        ).setColumnTypes(schema);
        DataFrame df = loader.load();

        PercentileClassifier pc = new PercentileClassifier("usage")
                .setPercentile(1.0);
        pc.process(df);
        DataFrame df_classified = pc.getResults();

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version"
        );
        APrioriSummarizer summ = new APrioriSummarizer();
        summ.setMinSupport(.01);
        summ.setMinRiskRatio(10.0);
        summ.setAttributes(explanationAttributes);
        summ.process(df_classified);

        Explanation e = summ.getResults();
        List<AttributeSet> results = e.getItemsets();
        assertEquals(20, e.getNumOutliers());
        assertEquals(1, results.size());
        assertEquals(0.5, results.get(0).getSupport(), 1e-10);
        Map<String, String> firstResult = results.get(0).getItems();
        HashSet<String> values = new HashSet<>();
        values.addAll(firstResult.values());
        assertTrue(values.contains("CAN"));
        assertTrue(values.contains("v3"));
    }

    @Test
    public void testSimpleCube() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("count", Schema.ColType.DOUBLE);
        schema.put("mean", Schema.ColType.DOUBLE);
        schema.put("std", Schema.ColType.DOUBLE);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/sample_cubed.csv"
        ).setColumnTypes(schema);
        DataFrame df = loader.load();

        ArithmeticClassifier ac = new ArithmeticClassifier("count", "mean", "std")
                .setPercentile(1.0);
        ac.setCountColumnName("count");
        ac.setIncludeHigh(false);
        ac.process(df);
        DataFrame df_classified = ac.getResults();

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version"
        );
        APrioriSummarizer summ = new APrioriSummarizer();
        summ.setCountColumn("count");
        summ.setMinSupport(.01);
        summ.setMinRiskRatio(10.0);
        summ.setAttributes(explanationAttributes);
        summ.process(df_classified);

        Explanation e = summ.getResults();
        List<AttributeSet> results = e.getItemsets();
        assertEquals(10, e.getNumOutliers());
        assertEquals(1, results.size());
        assertEquals(1.0, results.get(0).getSupport(), 1e-10);
        Map<String, String> firstResult = results.get(0).getItems();
        HashSet<String> values = new HashSet<>();
        values.addAll(firstResult.values());
        assertTrue(values.contains("CAN"));
        assertTrue(values.contains("v3"));
    }

    public void testGenCandidates() {
        HashSet<Integer> singleCandidates = new HashSet<>();
        for (int i = 1; i <= 4; i++) {
            singleCandidates.add(i);
        }
        HashSet<IntSet> o2Candidates = new HashSet<IntSet>();
        o2Candidates.add(new IntSet(1, 2));
        o2Candidates.add(new IntSet(2, 3));
        o2Candidates.add(new IntSet(1, 3));
        o2Candidates.add(new IntSet(3, 4));
        HashSet<IntSet> o3Candidates = APrioriSummarizer.getOrder3Candidates(o2Candidates, singleCandidates);
        assertEquals(1, o3Candidates.size());
        assertEquals(new IntSet(1,2,3), o3Candidates.iterator().next());
    }
}
package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class RegressionSummarizerTest {
    @Test
    public void testSimpleCube() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("count", Schema.ColType.DOUBLE);
        schema.put("mean", Schema.ColType.DOUBLE);
        schema.put("std", Schema.ColType.DOUBLE);
        schema.put("max", Schema.ColType.DOUBLE);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/sample_cubed.csv"
        ).setColumnTypes(schema);
        DataFrame df = loader.load();

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version"
        );
        RegressionSummarizer summ = new RegressionSummarizer();
        summ.setCountColumn("count");
        summ.setMeanColumn("mean");
        summ.setMaxColumn("max");
        summ.setMinCount(10);
        summ.setMinStd(2);
        summ.setAttributes(explanationAttributes);
        summ.process(df);

        Explanation e = summ.getResults();
        List<AttributeSet> results = e.getItemsets();
//        assertEquals(10, e.getNumOutliers());
//        assertEquals(1, results.size());
//        assertEquals(1.0, results.get(0).getSupport(), 1e-10);
//        Map<String, String> firstResult = results.get(0).getItems();
//        HashSet<String> values = new HashSet<>();
//        values.addAll(firstResult.values());
//        assertTrue(values.contains("CAN"));
//        assertTrue(values.contains("v3"));
    }
}
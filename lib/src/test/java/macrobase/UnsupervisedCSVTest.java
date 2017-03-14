package macrobase;

import macrobase.analysis.classify.PercentileClassifier;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.datamodel.DataFrame;
import macrobase.datamodel.Schema;
import macrobase.ingest.DataFrameCSVLoader;
import macrobase.ingest.FastDataFrameCSVLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

public class UnsupervisedCSVTest {
    @Test
    public void  testGetSummaries() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("usage", Schema.ColType.DOUBLE);
        schema.put("latency", Schema.ColType.DOUBLE);
        schema.put("location", Schema.ColType.STRING);
        schema.put("version", Schema.ColType.STRING);
        DataFrameCSVLoader loader = new FastDataFrameCSVLoader(
                "src/test/resources/sample.csv"
        ).setColumnTypes(schema);

        DataFrame df = loader.load();

        PercentileClassifier pc = new PercentileClassifier("usage")
                .setPercentile(1.0);
        pc.process(df);
        DataFrame df_classified = pc.getResults();

        BatchSummarizer summ = new BatchSummarizer()
                .setAttributes(Arrays.asList("location", "version"));
        summ.process(df_classified);
        Summary results = summ.getResults();

        assertEquals(3, results.getItemsets().size());
        System.out.println(results.prettyPrint());
    }
}

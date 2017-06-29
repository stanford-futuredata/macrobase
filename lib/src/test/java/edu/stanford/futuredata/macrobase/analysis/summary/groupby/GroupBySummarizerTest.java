package edu.stanford.futuredata.macrobase.analysis.summary.groupby;

import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.ItemsetBatchSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GroupBySummarizerTest {
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
        GroupBySummarizer summ = new GroupBySummarizer();
        summ.setMinSupport(.01);
        summ.setMinRiskRatio(10.0);
        summ.setAttributes(explanationAttributes);
        summ.process(df_classified);
    }
}
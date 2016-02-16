package macrobase.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.ingest.CsvLoader;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.transform.ZeroToOneLinearTransformation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamingAnalyzerTest {
    private static final Logger log = LoggerFactory.getLogger(StreamingAnalyzerTest.class);

    /*
        N.B. These tests could use considerable love.
             Right now, they basically just catch changed behavior
             in our core analysis pipelines.
     */

    @Test
    public void testMADAnalyzer() throws Exception {
        StreamingAnalyzer analyzer = new StreamingAnalyzer();
        analyzer.setAlphaMCD(.05);
        analyzer.setTargetPercentile(.99);
        analyzer.forceUsePercentile(true);
        analyzer.setMinInlierRatio(1);
        analyzer.setMinSupport(.02);
        analyzer.setDecayRate(.01);
        analyzer.setWarmupCount(10);
        analyzer.useTupleCountDecay(true);
        analyzer.setModelRefreshPeriod(50);
        analyzer.setSummaryPeriod(50);
        analyzer.setInputReservoirSize(10);
        analyzer.setScoreReservoirSize(10);
        analyzer.setInlierItemSummarySize(1000);
        analyzer.setOutlierItemSummarySize(1000);
        analyzer.setSeedRand(true);

        CsvLoader loader = new CsvLoader();
        loader.connect("src/test/resources/data/simple.csv");

        AnalysisResult ar = analyzer.analyzeOnePass(loader,
                                                    Lists.newArrayList("A1", "A2", "A3", "A4"),
                                                    Lists.newArrayList("A5"),
                                                    Lists.newArrayList(),
                                                    "");

        assertEquals(1, ar.getItemSets().size());
        assertEquals(1, ar.getItemSets().get(0).getItems().size());
        assertEquals("A1", ar.getItemSets().get(0).getItems().get(0).getColumn());
        assertEquals("0", ar.getItemSets().get(0).getItems().get(0).getValue());
    }

    @Test
    public void testMCDAnalyzer() throws Exception {
        StreamingAnalyzer analyzer = new StreamingAnalyzer();
        analyzer.setAlphaMCD(.05);
        analyzer.setTargetPercentile(.99);
        analyzer.forceUsePercentile(true);
        analyzer.setMinInlierRatio(1);
        analyzer.setMinSupport(.05);
        analyzer.setDecayRate(.01);
        analyzer.setWarmupCount(30);
        analyzer.useTupleCountDecay(true);
        analyzer.setModelRefreshPeriod(50);
        analyzer.setSummaryPeriod(50);
        analyzer.setInputReservoirSize(10);
        analyzer.setScoreReservoirSize(10);
        analyzer.setInlierItemSummarySize(1000);
        analyzer.setOutlierItemSummarySize(1000);
        analyzer.setSeedRand(true);

        CsvLoader loader = new CsvLoader();
        loader.connect("src/test/resources/data/simple.csv");
        AnalysisResult ar = analyzer.analyzeOnePass(loader,
                                                    Lists.newArrayList("A1", "A2", "A3"),
                                                    Lists.newArrayList("A4", "A5"),
                                                    Lists.newArrayList(),
                                                    "");

        assertEquals(1, ar.getItemSets().size());
        assertEquals(1, ar.getItemSets().get(0).getItems().size());
        assertEquals("A1", ar.getItemSets().get(0).getItems().get(0).getColumn());
        assertEquals("0", ar.getItemSets().get(0).getItems().get(0).getValue());
    }
}

package macrobase.analysis;

import com.google.common.collect.Lists;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.TARGET_PERCENTILE, 0.99);
        conf.set(MacroBaseConf.USE_PERCENTILE, true);
        conf.set(MacroBaseConf.MIN_OI_RATIO, 1);
        conf.set(MacroBaseConf.MIN_SUPPORT, .02);
        conf.set(MacroBaseConf.RANDOM_SEED, 0);
        conf.set(MacroBaseConf.DECAY_RATE, .01);
        conf.set(MacroBaseConf.WARMUP_COUNT, 10);
        conf.set(MacroBaseConf.USE_TUPLE_COUNT_PERIOD, true);
        conf.set(MacroBaseConf.USE_REAL_TIME_PERIOD, false);
        conf.set(MacroBaseConf.MODEL_UPDATE_PERIOD, 50);
        conf.set(MacroBaseConf.SUMMARY_UPDATE_PERIOD, 50);
        conf.set(MacroBaseConf.INPUT_RESERVOIR_SIZE, 10);
        conf.set(MacroBaseConf.SCORE_RESERVOIR_SIZE, 10);
        conf.set(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE, 1000);
        conf.set(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE, 1000);

        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3", "A4"));
        conf.set(MacroBaseConf.LOW_METRICS, Lists.newArrayList("A5"));
        conf.set(MacroBaseConf.HIGH_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "");

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataLoaderType.CSV_LOADER);
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        conf.loadSystemProperties();
        conf.sanityCheckStreaming();

        AnalysisResult ar = (new StreamingAnalyzer(conf)).analyzeOnePass();

        ar.getItemSets().get(0).prettyPrint();

        assertEquals(1, ar.getItemSets().size());
        assertEquals(1, ar.getItemSets().get(0).getItems().size());
        assertEquals("A1", ar.getItemSets().get(0).getItems().get(0).getColumn());
        assertEquals("0", ar.getItemSets().get(0).getItems().get(0).getValue());
    }

    @Test
    public void testMCDAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.TARGET_PERCENTILE, 0.99);
        conf.set(MacroBaseConf.USE_PERCENTILE, true);
        conf.set(MacroBaseConf.MIN_OI_RATIO, 1);
        conf.set(MacroBaseConf.MIN_SUPPORT, .05);
        conf.set(MacroBaseConf.RANDOM_SEED, 0);
        conf.set(MacroBaseConf.DECAY_RATE, .01);
        conf.set(MacroBaseConf.WARMUP_COUNT, 30);
        conf.set(MacroBaseConf.USE_TUPLE_COUNT_PERIOD, true);
        conf.set(MacroBaseConf.USE_REAL_TIME_PERIOD, false);
        conf.set(MacroBaseConf.MODEL_UPDATE_PERIOD, 50);
        conf.set(MacroBaseConf.SUMMARY_UPDATE_PERIOD, 50);
        conf.set(MacroBaseConf.INPUT_RESERVOIR_SIZE, 10);
        conf.set(MacroBaseConf.SCORE_RESERVOIR_SIZE, 10);
        conf.set(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE, 1000);
        conf.set(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE, 1000);

        conf.set(MacroBaseConf.MCD_ALPHA, .05);
        conf.set(MacroBaseConf.MCD_STOPPING_DELTA, 1e-3);

        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3"));
        conf.set(MacroBaseConf.LOW_METRICS, Lists.newArrayList("A4", "A5"));
        conf.set(MacroBaseConf.HIGH_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "");

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataLoaderType.CSV_LOADER);
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        conf.loadSystemProperties();
        conf.sanityCheckStreaming();

        AnalysisResult ar = (new StreamingAnalyzer(conf)).analyzeOnePass();

        assertEquals(1, ar.getItemSets().size());
        assertEquals(1, ar.getItemSets().get(0).getItems().size());
        assertEquals("A1", ar.getItemSets().get(0).getItems().get(0).getColumn());
        assertEquals("0", ar.getItemSets().get(0).getItems().get(0).getValue());
    }
}

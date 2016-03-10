package macrobase.pipeline;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.pipeline.BasicOneShotEWStreamingPipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.result.ColumnValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicOneShotEWStreamingPipelineTest {
    private static final Logger log = LoggerFactory.getLogger(BasicOneShotEWStreamingPipelineTest.class);

    /*
        N.B. These tests could use considerable love.
             Right now, they basically just catch changed behavior
             in our core analysis pipelines.
     */

    @Test
    public void testMADAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, 1)
                .set(MacroBaseConf.MIN_SUPPORT, .02)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.DECAY_RATE, .01) // streaming
                .set(MacroBaseConf.WARMUP_COUNT, 10)
                .set(MacroBaseConf.DECAY_TYPE, MacroBaseConf.PeriodType.TUPLE_BASED)
                .set(MacroBaseConf.MODEL_UPDATE_PERIOD, 50)
                .set(MacroBaseConf.SUMMARY_UPDATE_PERIOD, 50)
                .set(MacroBaseConf.INPUT_RESERVOIR_SIZE, 10)
                .set(MacroBaseConf.SCORE_RESERVOIR_SIZE, 10)
                .set(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3", "A4")) // loader
                .set(MacroBaseConf.LOW_METRICS, Lists.newArrayList("A5"))
                .set(MacroBaseConf.HIGH_METRICS, new ArrayList<>())
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        conf.loadSystemProperties();
        conf.sanityCheckStreaming();

        BasicOneShotEWStreamingPipeline sa = new BasicOneShotEWStreamingPipeline(conf);
        assertTrue(sa.hasNext());
        AnalysisResult ar = sa.next();

        log.debug(ar.toString());
        ar.getItemSets().get(0).prettyPrint();

        assertEquals(1, ar.getItemSets().size());
        assertEquals(1, ar.getItemSets().get(0).getItems().size());
        assertEquals("A1", ar.getItemSets().get(0).getItems().get(0).getColumn());
        assertEquals("0", ar.getItemSets().get(0).getItems().get(0).getValue());
    }

    @Test
    public void testMCDAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, 1)
                .set(MacroBaseConf.MIN_SUPPORT, .06)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.DECAY_RATE, .01) // streaming
                .set(MacroBaseConf.WARMUP_COUNT, 30)
                .set(MacroBaseConf.DECAY_TYPE, MacroBaseConf.PeriodType.TUPLE_BASED)
                .set(MacroBaseConf.MODEL_UPDATE_PERIOD, 50)
                .set(MacroBaseConf.SUMMARY_UPDATE_PERIOD, 50)
                .set(MacroBaseConf.INPUT_RESERVOIR_SIZE, 10)
                .set(MacroBaseConf.SCORE_RESERVOIR_SIZE, 10)
                .set(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.MCD_ALPHA, .5) // outlier detector
                .set(MacroBaseConf.MCD_STOPPING_DELTA, 1e-3)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3")) // loader
                .set(MacroBaseConf.LOW_METRICS, Lists.newArrayList("A4", "A5"))
                .set(MacroBaseConf.HIGH_METRICS, new ArrayList<>())
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        conf.loadSystemProperties();
        conf.sanityCheckStreaming();

        AnalysisResult ar = (new BasicOneShotEWStreamingPipeline(conf)).next();

        log.debug(ar.toString());

        assertEquals(1, ar.getItemSets().size());
        assertEquals(1, ar.getItemSets().get(0).getItems().size());
        assertEquals("A1", ar.getItemSets().get(0).getItems().get(0).getColumn());
        assertEquals("0", ar.getItemSets().get(0).getItems().get(0).getValue());

        assertEquals(BasePipelineTest.countLines("src/test/resources/data/simple.csv"),
                     ar.getNumInliers()+ar.getNumOutliers(), 0);
    }

    @Test
    public void testSensor10KPower() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, 1)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.DECAY_RATE, .01) // streaming
                .set(MacroBaseConf.WARMUP_COUNT, 100)
                .set(MacroBaseConf.DECAY_TYPE, MacroBaseConf.PeriodType.TUPLE_BASED)
                .set(MacroBaseConf.MODEL_UPDATE_PERIOD, 1000)
                .set(MacroBaseConf.SUMMARY_UPDATE_PERIOD, 1000)
                .set(MacroBaseConf.INPUT_RESERVOIR_SIZE, 1000)
                .set(MacroBaseConf.SCORE_RESERVOIR_SIZE, 1000)
                .set(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("device_id")) // loader
                .set(MacroBaseConf.LOW_METRICS, new ArrayList<>())
                .set(MacroBaseConf.HIGH_METRICS, Lists.newArrayList("power_drain"))
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/sensor10k.csv.gz");

        conf.loadSystemProperties();
        conf.sanityCheckBatch();

        AnalysisResult ar = (new BasicOneShotEWStreamingPipeline(conf)).next();

        assertTrue(ar.getLoadTime() >= 0);
        assertTrue(ar.getExecutionTime() >= 0);
        assertTrue(ar.getSummarizationTime() >= 0);

        assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("device_id");
        HashSet<String> toFindValue = Sets.newHashSet("2040");

        for (ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());
    }

    @Test
    public void testSensor10KTemp() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, 1)
                .set(MacroBaseConf.MIN_SUPPORT, .06)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.DECAY_RATE, .01) // streaming
                .set(MacroBaseConf.WARMUP_COUNT, 100)
                .set(MacroBaseConf.DECAY_TYPE, MacroBaseConf.PeriodType.TUPLE_BASED)
                .set(MacroBaseConf.MODEL_UPDATE_PERIOD, 1000)
                .set(MacroBaseConf.SUMMARY_UPDATE_PERIOD, 1000)
                .set(MacroBaseConf.INPUT_RESERVOIR_SIZE, 1000)
                .set(MacroBaseConf.SCORE_RESERVOIR_SIZE, 1000)
                .set(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE, 1000)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("device_id", "model", "firmware_version")) // loader
                .set(MacroBaseConf.LOW_METRICS, Lists.newArrayList("temperature"))
                .set(MacroBaseConf.HIGH_METRICS, new ArrayList<>())
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/sensor10k.csv.gz");

        conf.loadSystemProperties();
        conf.sanityCheckBatch();

        AnalysisResult ar = (new BasicOneShotEWStreamingPipeline(conf)).next();

        log.debug(ar.toString());

        assertEquals(3, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("model", "firmware_version");
        HashSet<String> toFindValue = Sets.newHashSet("M101", "0.4");

        for (ColumnValue cv : ar.getItemSets().get(2).getItems()) {
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());
    }
}

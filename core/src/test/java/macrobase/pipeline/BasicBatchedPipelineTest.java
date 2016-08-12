package macrobase.pipeline;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.pipeline.BasicBatchedPipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.result.ColumnValue;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.*;

public class BasicBatchedPipelineTest {
    private static final Logger log = LoggerFactory.getLogger(BasicBatchedPipelineTest.class);

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
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3", "A4")) // loader
                .set(MacroBaseConf.METRICS, Lists.newArrayList("A5"))
                .set(MacroBaseConf.LOW_METRIC_TRANSFORM, Lists.newArrayList("A5"))
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        conf.loadSystemProperties();

        BasicBatchedPipeline ba = new BasicBatchedPipeline();
        ba.initialize(conf);
        AnalysisResult ar = ba.run().get(0);

        assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("A1", "A2", "A3", "A4");
        HashSet<String> toFindValue = Sets.newHashSet("0", "1", "2", "3");

        for (ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());

        assertEquals(BasePipelineTest.countLines("src/test/resources/data/simple.csv"),
                     ar.getNumInliers()+ar.getNumOutliers(), 0);
    }


    @Test
    public void testSensor10KPower() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("device_id")) // loader
                .set(MacroBaseConf.METRICS, Lists.newArrayList("power_drain"))
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/sensor10k.csv.gz");

        conf.loadSystemProperties();

        BasicBatchedPipeline ba = new BasicBatchedPipeline();
        ba.initialize(conf);
        AnalysisResult ar = ba.run().get(0);

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
                .set(MacroBaseConf.MIN_OI_RATIO, 3)
                .set(MacroBaseConf.MIN_SUPPORT, .5)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("device_id", "model", "firmware_version")) // loader
                .set(MacroBaseConf.METRICS, Lists.newArrayList("temperature"))
                .set(MacroBaseConf.LOW_METRIC_TRANSFORM, Lists.newArrayList("temperature"))
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/sensor10k.csv.gz");

        conf.loadSystemProperties();

        BasicBatchedPipeline ba = new BasicBatchedPipeline();
        ba.initialize(conf);
        AnalysisResult ar = ba.run().get(0);

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


    @Test
    public void testMCDAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.99) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.MCD_ALPHA, .05)
                .set(MacroBaseConf.MCD_STOPPING_DELTA, 1e-3)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3")) // loader
                .set(MacroBaseConf.METRICS, Lists.newArrayList("A4", "A5"))
                .set(MacroBaseConf.LOW_METRIC_TRANSFORM, Lists.newArrayList("A4", "A5"))
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        conf.loadSystemProperties();

        BasicBatchedPipeline ba = new BasicBatchedPipeline();
        ba.initialize(conf);
        AnalysisResult ar = ba.run().get(0);

        assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("A1", "A2", "A3");
        HashSet<String> toFindValue = Sets.newHashSet("0", "1", "2");

        for (ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());
    }
}

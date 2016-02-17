package macrobase.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.MacroBase;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.CsvLoader;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.transform.ZeroToOneLinearTransformation;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.*;

public class BatchAnalyzerTest {
    private static final Logger log = LoggerFactory.getLogger(BatchAnalyzerTest.class);

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
        conf.set(MacroBaseConf.MIN_OI_RATIO, .01);
        conf.set(MacroBaseConf.MIN_SUPPORT, .01);
        conf.set(MacroBaseConf.RANDOM_SEED, 0);

        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3", "A4"));
        conf.set(MacroBaseConf.LOW_METRICS, Lists.newArrayList("A5"));
        conf.set(MacroBaseConf.HIGH_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "");

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataLoaderType.CSV_LOADER);
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        AnalysisResult ar = (new BatchAnalyzer(conf)).analyze();

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
    }

    @Test
    public void testMCDAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.TARGET_PERCENTILE, 0.99);
        conf.set(MacroBaseConf.USE_PERCENTILE, true);
        conf.set(MacroBaseConf.MIN_OI_RATIO, .01);
        conf.set(MacroBaseConf.MIN_SUPPORT, .01);
        conf.set(MacroBaseConf.RANDOM_SEED, 0);
        conf.set(MacroBaseConf.MCD_ALPHA, .05);
        conf.set(MacroBaseConf.MCD_STOPPING_DELTA, 1e-3);

        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("A1", "A2", "A3"));
        conf.set(MacroBaseConf.LOW_METRICS, Lists.newArrayList("A4", "A5"));
        conf.set(MacroBaseConf.HIGH_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "");

        conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataLoaderType.CSV_LOADER);
        conf.set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/simple.csv");

        AnalysisResult ar = (new BatchAnalyzer(conf)).analyze();

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

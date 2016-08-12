package macrobase.analysis.pipeline;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.transform.GridDumpingBatchScoreTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.diagnostics.ScoreDumper;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.result.ColumnValue;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GridDumpingPipelineTest {
    private static final Logger log = LoggerFactory.getLogger(GridDumpingPipeline.class);

    @Test
    public void testBayesianNormalAnalyzer() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "macrobase.analysis.stats.BayesianNormalDensity")
                .set(MacroBaseConf.TARGET_PERCENTILE, 0.95) // analysis
                .set(MacroBaseConf.USE_PERCENTILE, true)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("XX")) // loader
                .set(MacroBaseConf.METRICS, Lists.newArrayList("XX"))
                .set(ScoreDumper.SCORED_DATA_FILE, "tmp.json")
                .set(GridDumpingBatchScoreTransform.DUMP_SCORE_GRID, "grid.json")
                .set(GridDumpingBatchScoreTransform.NUM_SCORE_GRID_POINTS_PER_DIMENSION, 20)
                .set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER)
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/20points.csv");

        conf.loadSystemProperties();

        AnalysisResult ar = (new GridDumpingPipeline().initialize(conf)).run().get(0);
        Assert.assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("XX");
        HashSet<String> toFindValue = Sets.newHashSet("-5.8");


        for (ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            log.debug("column {}", cv.getColumn());
            Assert.assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            log.debug("value {}", cv.getValue());
            Assert.assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        Assert.assertEquals(0, toFindColumn.size());
        Assert.assertEquals(0, toFindValue.size());

        Assert.assertEquals(ar.getNumInliers(), 19, 1e-9);
        Assert.assertEquals(ar.getNumOutliers(), 1, 1e-9);
    }
}

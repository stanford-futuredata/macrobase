package macrobase.pipeline;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.pipeline.GridDumpingPipeline;
import macrobase.analysis.pipeline.MixtureModelPipeline;
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

public class MixtureModelPipelineTest {
    private static final Logger log = LoggerFactory.getLogger(GridDumpingPipeline.class);

    @Test
    public void test2ClusterOutliers() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, MacroBaseConf.TransformType.VARIATIONAL_GMM)
                .set(MacroBaseConf.USE_PERCENTILE, true) // Forced to pick.
                .set(MacroBaseConf.NUM_MIXTURES, 2)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("Cluster")) // loader
                .set(MacroBaseConf.LOW_METRICS, new ArrayList<>())
                .set(MacroBaseConf.HIGH_METRICS, "XX")
                .set(MacroBaseConf.SCORED_DATA_FILE, "tmp.json")
                .set(MacroBaseConf.DUMP_SCORE_GRID, "grid.json")
                .set(MacroBaseConf.TARGET_GROUP, "2")
                .set(MacroBaseConf.NUM_SCORE_GRID_POINTS_PER_DIMENSION, 20)
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv");

        conf.loadSystemProperties();
        conf.sanityCheckBatch();

        AnalysisResult ar = (new MixtureModelPipeline().initialize(conf)).run().get(0);
        assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("Cluster");
        HashSet<String> toFindValue = Sets.newHashSet("first");


        for (ColumnValue cv : ar.getItemSets().get(0).getItems()) {
            log.debug("column {}", cv.getColumn());
            assertTrue(toFindColumn.contains(cv.getColumn()));
            toFindColumn.remove(cv.getColumn());
            log.debug("value {}", cv.getValue());
            assertTrue(toFindValue.contains(cv.getValue()));
            toFindValue.remove(cv.getValue());
        }

        assertEquals(0, toFindColumn.size());
        assertEquals(0, toFindValue.size());

        assertEquals(ar.getNumInliers(), 9, 1e-9);
        assertEquals(ar.getNumOutliers(), 9, 1e-9);
    }

    @Test
    public void testWellSeparatedOutliers() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, MacroBaseConf.TransformType.VARIATIONAL_GMM)
                .set(MacroBaseConf.USE_PERCENTILE, true) // Forced to pick.
                .set(MacroBaseConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("XX")) // loader
                .set(MacroBaseConf.LOW_METRICS, new ArrayList<>())
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.SCORED_DATA_FILE, "tmp.json")
                .set(MacroBaseConf.DUMP_SCORE_GRID, "grid.json")
                .set(MacroBaseConf.TARGET_GROUP, "2, 11")
                .set(MacroBaseConf.NUM_SCORE_GRID_POINTS_PER_DIMENSION, 20)
                .set(MacroBaseConf.AUXILIARY_ATTRIBUTES, "")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz");

        conf.loadSystemProperties();
        conf.sanityCheckBatch();

        AnalysisResult ar = (new MixtureModelPipeline().initialize(conf)).run().get(0);
        assertEquals(0, ar.getItemSets().size());

        assertEquals(ar.getNumInliers(), 500, 1e-9);
        assertEquals(ar.getNumOutliers(), 200, 1e-9);
    }
}

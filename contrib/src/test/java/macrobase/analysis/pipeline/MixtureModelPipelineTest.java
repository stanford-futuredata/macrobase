package macrobase.analysis.pipeline;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.mixture.GMMConf;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MixtureModelPipelineTest {
    private static final Logger log = LoggerFactory.getLogger(GridDumpingPipeline.class);

    @Test
    public void test2ClusterOutliersWithTargetGroup() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "macrobase.analysis.stats.mixture.FiniteGMM")
                .set(MacroBaseConf.USE_PERCENTILE, true) // Forced to pick.
                .set(GMMConf.NUM_MIXTURES, 2)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("Cluster")) // loader
                .set(MacroBaseConf.METRICS, "XX")
                .set(ScoreDumper.SCORED_DATA_FILE, "tmp.json")
                .set(GridDumpingBatchScoreTransform.DUMP_SCORE_GRID, "grid.json")
                .set(GMMConf.TARGET_GROUP, "2")
                .set(GridDumpingBatchScoreTransform.NUM_SCORE_GRID_POINTS_PER_DIMENSION, 20)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv");

        conf.loadSystemProperties();

        AnalysisResult ar = (new MixtureModelPipeline().initialize(conf)).run().get(0);
        Assert.assertEquals(1, ar.getItemSets().size());

        HashSet<String> toFindColumn = Sets.newHashSet("Cluster");
        HashSet<String> toFindValue = Sets.newHashSet("first");


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

        Assert.assertEquals(ar.getNumInliers(), 9, 1e-9);
        Assert.assertEquals(ar.getNumOutliers(), 9, 1e-9);
    }

    /**
     * After fitting a mixture of Gaussians, runs the summarizer
     * on all of the clusters and make sure each cluster has the
     * correct summary.
     * @throws Exception
     */
    @Test
    public void test2ClusterOutliersWithoutTargetGroup() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "macrobase.analysis.stats.mixture.FiniteGMM")
                .set(MacroBaseConf.USE_PERCENTILE, true) // Forced to pick.
                .set(GMMConf.NUM_MIXTURES, 2)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("Cluster")) // loader
                .set(MacroBaseConf.METRICS, "XX")
                .set(ScoreDumper.SCORED_DATA_FILE, "tmp.json")
                .set(GridDumpingBatchScoreTransform.DUMP_SCORE_GRID, "grid.json")
                .set(GridDumpingBatchScoreTransform.NUM_SCORE_GRID_POINTS_PER_DIMENSION, 20)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv");

        conf.loadSystemProperties();

        List<AnalysisResult> results = (new MixtureModelPipeline().initialize(conf)).run();
        Assert.assertEquals(2, results.size());

        HashSet<String> toFindValue = Sets.newHashSet("first", "second");

        for (int i = 0; i < results.size(); i++) {
            AnalysisResult ar = results.get(i);
            Assert.assertEquals(1, ar.getItemSets().size());
            ItemsetResult is = ar.getItemSets().get(0);

            for (ColumnValue cv : is.getItems()) {
                log.debug("column {}", cv.getColumn());
                Assert.assertEquals("Cluster", cv.getColumn());
                log.debug("value {}", cv.getValue());
                Assert.assertTrue(toFindValue.contains(cv.getValue()));
                toFindValue.remove(cv.getValue());
            }

            Assert.assertEquals(ar.getNumInliers(), 9, 1e-9);
            Assert.assertEquals(ar.getNumOutliers(), 9, 1e-9);
        }

        Assert.assertEquals(0, toFindValue.size());
    }

    @Test
    public void testWellSeparatedOutliers() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "macrobase.analysis.stats.mixture.FiniteGMM")
                .set(MacroBaseConf.USE_PERCENTILE, true) // Forced to pick.
                .set(GMMConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.MIN_OI_RATIO, .01)
                .set(MacroBaseConf.MIN_SUPPORT, .01)
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("XX")) // loader
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(ScoreDumper.SCORED_DATA_FILE, "tmp.json")
                .set(GridDumpingBatchScoreTransform.DUMP_SCORE_GRID, "grid.json")
                .set(GMMConf.TARGET_GROUP, "2, 11")
                .set(GridDumpingBatchScoreTransform.NUM_SCORE_GRID_POINTS_PER_DIMENSION, 20)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz");

        conf.loadSystemProperties();

        AnalysisResult ar = (new MixtureModelPipeline().initialize(conf)).run().get(0);
        Assert.assertEquals(0, ar.getItemSets().size());

        Assert.assertEquals(ar.getNumInliers(), 500, 1e-9);
        Assert.assertEquals(ar.getNumOutliers(), 200, 1e-9);
    }
}

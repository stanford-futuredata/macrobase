package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.JsonUtils;
import macrobase.diagnostics.ScoreDumper;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class DPGMMTest {
    private static final Logger log = LoggerFactory.getLogger(DPGMMTest.class);

    @Test
    /**
     * Tests Bayesian Dirichlet Process Gaussian Mixture on a three well separated clusters.
     */
    public void univariateToyBimodalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_DPMM")
                .set(GMMConf.MAX_ITERATIONS_TO_CONVERGE, 15)
                .set(GMMConf.DPM_TRUNCATING_PARAMETER, 8)
                .set(GMMConf.DPM_CONCENTRATION_PARAMETER, 2)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv")
                .set(MacroBaseConf.METRICS, "XX")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = conf.constructIngester().getStream().drain();
        assertEquals(18, data.size());

        DPGMM variationalDPGM = new DPGMM(conf);
        variationalDPGM.train(data);
        log.debug("{}",  variationalDPGM.getClusterCenters(), variationalDPGM.getClusterProportions());
    }

    @Test
    /**
     * Tests Bayesian Gaussian Mixture Model on a three well separated clusters.
     */
    public void bivariateWellSeparatedNormalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 40)
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_DPMM")
                .set(GMMConf.DPM_TRUNCATING_PARAMETER, 10)
                .set(ScoreDumper.SCORED_DATA_FILE, "tmp.csv")
                .set(GMMConf.MAX_ITERATIONS_TO_CONVERGE, 15)
                .set(GMMConf.DPM_CONCENTRATION_PARAMETER, 0.2)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = conf.constructIngester().getStream().drain();
        assertEquals(700, data.size());

        DPGMM variationalDPGM = new DPGMM(conf);
        variationalDPGM.train(data);

        log.debug("clusters : {}", variationalDPGM.getClusterCenters());
        log.debug("weights: {}", variationalDPGM.getClusterProportions());

        double[][] boundaries = {
                {-1, 13.01},
                {-1, 13.01},
        };

        JsonUtils.dumpAsJson(variationalDPGM.getClusterCovariances(), "DPGMMTest-bivariateWellSeparatedNormalTest-covariances.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterCenters(), "DPGMMTest-bivariateWellSeparatedNormalTest-means.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterProportions(), "DPGMMTest-bivariateWellSeparatedNormalTest-weights.json");

        conf.set(GMMConf.SCORE_DUMP_FILE_CONFIG_PARAM, "3gaussians-700-grid.json");
        ScoreDumper dumper = new ScoreDumper(conf);
        dumper.dumpScores(variationalDPGM, boundaries, 0.05);

        conf.set(GMMConf.SCORE_DUMP_FILE_CONFIG_PARAM, "3gaussians-700-data.json");
        dumper = new ScoreDumper(conf);
        dumper.dumpScores(variationalDPGM, data);


        double[][] clusterMeans = {
                {2, 11},
                {1, 1},
                {10, 3},
        };
        double[] clusterPoints = {
                200,
                200,
                300,
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k = 0; k < 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }

        List<RealVector> calculatedCenters = variationalDPGM.getClusterCenters();
        double[] weights = variationalDPGM.getClusterProportions();

        List<List<Integer>> lists = new ArrayList<>();
        lists.add(new ArrayList<>());
        lists.add(new ArrayList<>());
        lists.add(new ArrayList<>());

        double[] inferedWeights = new double[3];
        for (int i = 0 ; i < calculatedCenters.size(); i++ ) {
            if (weights[i] < 0.001) {
                continue;
            }
            for (int j=0 ; j < 3 ; j++) {
                if (calculatedCenters.get(i).getDistance(vectorClusterMeans.get(j)) < 2) {
                    log.debug("identified cluster {} to be close to {} (weight = {})", calculatedCenters.get(i), vectorClusterMeans.get(j), weights[i]);
                    inferedWeights[j] += weights[i];
                    lists.get(j).add(i);
                    break;
                }
            }
        }

        for (int j=0 ; j < 3 ; j++) {
            assertEquals(clusterPoints[j], inferedWeights[j] * 700, 3);
        }
        // Make sure centers belong to only one cluster.
        double[] maxProbas = {0, 0, 0};
        for (int i = 0; i < 3; i++) {
            double[] probas = variationalDPGM.getClusterProbabilities(new Datum(new ArrayList<Integer>(), vectorClusterMeans.get(i)));
            log.debug("probas = {}", probas);
            for (int j=0; j< 3; j++) {
                double p = 0;
                for (int ii : lists.get(j)) {
                    p += probas[ii];
                }
                maxProbas[j] = Math.max(p, maxProbas[j]);
            }
        }
        for (int j=0; j< 3; j++) {
            assertEquals(1, maxProbas[j], 0.01);
        }
    }

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void bivariateOkSeparatedNormalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_DPMM")
                .set(GMMConf.MAX_ITERATIONS_TO_CONVERGE, 15)
                .set(GMMConf.DPM_TRUNCATING_PARAMETER, 20)
                .set(GMMConf.DPM_CONCENTRATION_PARAMETER, 0.1)
                .set(GMMConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-7000points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = conf.constructIngester().getStream().drain();
        assertEquals(7000, data.size());

        double[][] clusterMeans = {
                {1.5, 2},
                {2, 0},
                {4.5, 1},
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k = 0; k < 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }
        double[][][] clusterCovariances = {
                {{0.5, 0.4}, {0.4, 0.5}},
                {{0.3, 0}, {0, 0.6}},
                {{0.9, 0.2}, {0.2, 0.3}},
        };

        double[][] boundaries = {
                {0, 6.01},
                {-2, 4.01},
        };

        DPGMM variationalDPGM = new DPGMM(conf);
        variationalDPGM.train(data);

        JsonUtils.dumpAsJson(variationalDPGM.getClusterCovariances(), "DPGMMTest-bivariateOkSeparatedNormalTest-covariances.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterCenters(), "DPGMMTest-bivariateOkSeparatedNormalTest-means.json");
        JsonUtils.dumpAsJson(variationalDPGM.getClusterProportions(), "DPGMMTest-bivariateOkSeparatedNormalTest-weights.json");

        conf.set(GMMConf.SCORE_DUMP_FILE_CONFIG_PARAM, "3gaussians-7k-grid.json");
        ScoreDumper dumper = new ScoreDumper(conf);
        dumper.dumpScores(variationalDPGM, boundaries, 0.05);

        conf.set(GMMConf.SCORE_DUMP_FILE_CONFIG_PARAM, "3gaussians-7k-data.json");
        dumper = new ScoreDumper(conf);
        dumper.dumpScores(variationalDPGM, data);
    }
}

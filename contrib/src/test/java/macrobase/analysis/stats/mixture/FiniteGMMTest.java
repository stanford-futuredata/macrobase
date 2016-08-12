package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.JsonUtils;
import macrobase.diagnostics.ScoreDumper;
import macrobase.ingest.CSVIngester;
import macrobase.util.DiagnosticsUtils;
import macrobase.util.Drainer;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class FiniteGMMTest {
    private static final Logger log = LoggerFactory.getLogger(FiniteGMMTest.class);

    @Test
    /**
     * Tests Bayesian Gaussian Mixture Model on a three well separated clusters.
     */
    public void univariateToyBimodalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "MEAN_FIELD_GMM")
                .set(GMMConf.NUM_MIXTURES, 2)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv")
                .set(MacroBaseConf.METRICS, "XX")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(18, data.size());

        FiniteGMM finiteGMM = new FiniteGMM(conf);
        finiteGMM.train(data);
    }

    @Test
    /**
     * Tests Bayesian Gaussian Mixture Model on a three well separated clusters.
     */
    public void bivariateWellSeparatedNormalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 44)
                .set(MacroBaseConf.TRANSFORM_TYPE, "MEAN_FIELD_GMM")
                .set(GMMConf.NUM_MIXTURES, 3)
                .set(GMMConf.MIXTURE_CENTERS_FILE, "src/test/resources/data/3gaussians-700.points-centers.json")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(700, data.size());

        FiniteGMM finiteGMM = new FiniteGMM(conf);
        List<RealVector> calculatedMeans;

        // Make sure we have 3 clusters. Sometimes initialization is not great.
        finiteGMM.train(data);

        calculatedMeans = finiteGMM.getClusterCenters();
        List<RealMatrix> calculatedCovariances = finiteGMM.getClusterCovariances();

        double[][] clusterMeans = {
                {2, 11},
                {1, 1},
                {10, 3},
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


        ExpectMaxGMM gmm = new ExpectMaxGMM(conf);
        gmm.train(data);
        List<RealVector> emMeans = gmm.getClusterCenters();
        List<RealMatrix> emCovariances = gmm.getClusterCovariances();

        for (int i = 0; i < 3; i++) {
            boolean identified = false;
            for (int j = 0; j < 3; j++) {
                if (calculatedMeans.get(i).getDistance(vectorClusterMeans.get(j)) < 0.1) {
                    for (int p = 0; p < 2; p++) {
                        for (int q = 0; q < 2; q++) {
                            // Make sure covariance is in the ballpark. Since we only had 700 points.
                            assertEquals(clusterCovariances[j][p][q], calculatedCovariances.get(i).getEntry(p, q), 0.6);
                        }
                    }
                    identified = true;
                    break;
                }
            }
            assertEquals(true, identified);
            for (int z = 0; z < 3; z++) {
                if (emMeans.get(z).getDistance(calculatedMeans.get(i)) < 0.1) {
                    for (int p = 0; p < 2; p++) {
                        for (int q = 0; q < 2; q++) {
                            // Make sure we have a closer estimate to EM algorithm means
                            assertEquals(emCovariances.get(z).getEntry(p, q), calculatedCovariances.get(i).getEntry(p, q), 0.1);
                        }
                    }
                    break;
                }
            }
        }

        // Make sure centers belong to only one cluster.
        double[] maxProbas = {0, 0, 0};
        for (int i = 0; i < 3; i++) {
            double[] probas = finiteGMM.getClusterProbabilities(new Datum(new ArrayList<Integer>(), vectorClusterMeans.get(i)));
            for (int j=0; j< 3; j++) {
                maxProbas[j] = Math.max(probas[j], maxProbas[j]);
            }
        }
        for (int j=0; j< 3; j++) {
            assertEquals(maxProbas[j], 1, 0.01);
        }
    }

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void bivariateOkSeparatedNormalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "MEAN_FIELD_GMM")
                .set(GMMConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-7000points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
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

        FiniteGMM finiteGMM = new FiniteGMM(conf);
        finiteGMM.train(data);

        List<RealVector> calculatedMeans = finiteGMM.getClusterCenters();
        List<RealMatrix> calculatedCovariances = finiteGMM.getClusterCovariances();

        for (int i = 0; i < 3; i++) {
            boolean identified = false;
            for (int j = 0; j < 3; j++) {
                if (calculatedMeans.get(i).getDistance(vectorClusterMeans.get(j)) < 0.1) {
                    for (int p = 0; p < 2; p++) {
                        for (int q = 0; q < 2; q++) {
                            assertEquals(clusterCovariances[j][p][q], calculatedCovariances.get(i).getEntry(p, q), 0.1);
                        }
                    }
                    identified = true;
                    break;
                }
            }
            assertEquals("a cluster was not identified", true, identified);
        }

        double[][] boundaries = {
                {0, 6.01},
                {-2, 4.01},
        };
        List<Datum> scoredData = DiagnosticsUtils.createGridFixedIncrement(boundaries, 0.05);

        JsonUtils.dumpAsJson(finiteGMM.getClusterCovariances(), "FiniteGMMTest-bivariateOkSeparatedNormalTest-covariances.json");
        JsonUtils.dumpAsJson(finiteGMM.getClusterCenters(), "FiniteGMMTest-bivariateOkSeparatedNormalTest-means.json");
        JsonUtils.dumpAsJson(finiteGMM.getPriorAdjustedClusterProportions(), "FiniteGMMTest-bivariateOkSeparatedNormalTest-weights.json");

        conf.set(GMMConf.SCORE_DUMP_FILE_CONFIG_PARAM, "3gaussians-7k-grid.json");
        ScoreDumper dumper = new ScoreDumper(conf);
        dumper.dumpScores(finiteGMM, scoredData);

        conf.set(GMMConf.SCORE_DUMP_FILE_CONFIG_PARAM, "3gaussians-7k-data.json");
        dumper = new ScoreDumper(conf);
        dumper.dumpScores(finiteGMM, data);
    }

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void unusedOrOverlappingClusterTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 4)
                .set(MacroBaseConf.TRANSFORM_TYPE, "MEAN_FIELD_GMM")
                .set(GMMConf.NUM_MIXTURES, 4)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2gaussians-500points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(500, data.size());

        FiniteGMM finiteGMM = new FiniteGMM(conf);
        List<RealVector> calculatedMeans = null;
        double[] calculatedWeights = null;

        int numClustersIdentified = 0;
        // Sometimes there is a weird convergence into one cluster when trying to fit more than 2 clusters.
        while (numClustersIdentified < 2) {
            finiteGMM.train(data);

            calculatedMeans = finiteGMM.getClusterCenters();
            calculatedWeights = finiteGMM.getPriorAdjustedClusterProportions();

            numClustersIdentified = 0;
            for (double weight : calculatedWeights) {
                if (weight > 0.1) {
                    numClustersIdentified += 1;
                }
            }
        }

        double[][] clusterMeans = {
                {1, 1},
                {10, 3},
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(2);
        for (int k = 0; k < 2; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }

        double[] clusterWeights = {
                200,
                300,
        };
        double[] identifiedWeights = {0, 0};
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 2; j++) {
                if (calculatedMeans.get(i).getDistance(vectorClusterMeans.get(j)) < 1) {
                    identifiedWeights[j] += calculatedWeights[i];
                    log.debug("adding {} to cluster {}", calculatedWeights[i], vectorClusterMeans.get(j));
                    break;
                }
            }
        }
        assertEquals(identifiedWeights[0] * 500, clusterWeights[0], 1);
        assertEquals(identifiedWeights[1] * 500, clusterWeights[1], 1);
    }
}

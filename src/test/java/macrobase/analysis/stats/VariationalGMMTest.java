package macrobase.analysis.stats;

import com.google.common.collect.Lists;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class VariationalGMMTest {
    private static final Logger log = LoggerFactory.getLogger(VariationalGMMTest.class);

    @Test
    /**
     * Tests Bayesian Gaussian Mixture Model on a three well separated clusters.
     */
    public void univariateToyBimodalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_GMM")
                .set(MacroBaseConf.NUM_MIXTURES, 2)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.UNCOMPRESSED)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/toy2gaussians.csv")
                .set(MacroBaseConf.HIGH_METRICS, "XX")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(18, data.size());

        VariationalGMM variationalGMM = new VariationalGMM(conf);
        variationalGMM.train(data);
    }

    @Test
    /**
     * Tests Bayesian Gaussian Mixture Model on a three well separated clusters.
     */
    public void bivariateWellSeparatedNormalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_GMM")
                .set(MacroBaseConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(700, data.size());

        VariationalGMM variationalGMM = new VariationalGMM(conf);
        List<RealVector> calculatedMeans;

        int numClustersIdentified = 0;
        // Make sure we have 3 clusters. Sometimes initialization is not great.
        while (numClustersIdentified < 3) {
            variationalGMM.train(data);

            double[] calculatedWeights = variationalGMM.getPriorAdjustedWeights();

            numClustersIdentified = 0;
            for (double weight : calculatedWeights) {
                if (weight > 0.1) {
                    numClustersIdentified += 1;
                }
            }

        }
        calculatedMeans = variationalGMM.getMeans();
        List<RealMatrix> calculatedCovariances = variationalGMM.getCovariances();

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


        GaussianMixtureModel gmm = new GaussianMixtureModel(conf);
        gmm.train(data);
        List<RealVector> emMeans = gmm.getMeans();
        List<RealMatrix> emCovariances = gmm.getCovariance();

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
    }

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void bivariateOkSeparatedNormalTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_GMM")
                .set(MacroBaseConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-7000points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
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

        VariationalGMM variationalGMM = new VariationalGMM(conf);
        variationalGMM.train(data);

        List<RealVector> calculatedMeans = variationalGMM.getMeans();
        List<RealMatrix> calculatedCovariances = variationalGMM.getCovariances();

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
    }

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void unusedOrOverlappingClusterTest() throws ConfigurationException, IOException, SQLException {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.TRANSFORM_TYPE, "VARIATIONAL_GMM")
                .set(MacroBaseConf.NUM_MIXTURES, 4)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/2gaussians-500points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Lists.newArrayList(conf.constructIngester());
        assertEquals(500, data.size());

        VariationalGMM variationalGMM = new VariationalGMM(conf);
        List<RealVector> calculatedMeans = null;
        double[] calculatedWeights = null;

        int numClustersIdentified = 0;
        // Sometimes there is a weird convergence into one cluster when trying to fit more than 2 clusters.
        while (numClustersIdentified < 2) {
            variationalGMM.train(data);

            calculatedMeans = variationalGMM.getMeans();
            calculatedWeights = variationalGMM.getPriorAdjustedWeights();

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
        assertEquals(identifiedWeights[0], clusterWeights[0], 1);
        assertEquals(identifiedWeights[1], clusterWeights[1], 1);
    }
}

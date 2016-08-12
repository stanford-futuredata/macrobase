package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
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

public class ExpectMaxGMMTest {
    private static final Logger log = LoggerFactory.getLogger(ExpectMaxGMMTest.class);

    @Test
    /**
     * Tests Gaussian Mixture Model on a three well separated clusters.
     */
    public void bivariateWellSeparatedNormalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 2)
                .set(MacroBaseConf.TRANSFORM_TYPE, "EM_GMM")
                .set(GMMConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(700, data.size());

        ExpectMaxGMM gmm = new ExpectMaxGMM(conf);
        gmm.train(data);

        double[][] clusterMeans = {
                {2, 11},
                {1, 1},
                {10, 3},
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k =0; k< 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }
        double [][][] clusterCovariances = {
                {{0.5,0.4},{0.4,0.5}},
                {{0.3,0},{0,0.6}},
                {{0.9,0.2},{0.2,0.3}},
        };

        List<RealVector> calculatedMeans = gmm.getClusterCenters();
        List<RealMatrix> calculatedCovariances = gmm.getClusterCovariances();

        for (int i=0; i<3 ; i ++) {
            boolean identified = false;
            for (int j=0; j<3; j++) {
                if (calculatedMeans.get(i).getDistance(vectorClusterMeans.get(j)) < 0.1 ) {
                    for (int p=0; p<2; p++) {
                        for (int q=0; q<2; q++) {
                            // Make sure covariance is in the ballpark. Since we only had 700 points.
                            assertEquals(clusterCovariances[j][p][q], calculatedCovariances.get(i).getEntry(p,q), 0.7);
                        }
                    }
                    identified = true;
                    break;
                }
            }
            assertEquals(true, identified);
        }
        // Make sure centers belong to only one cluster.
        double[] maxProbas = {0, 0, 0};
        for (int i = 0; i < 3; i++) {
            double[] probas = gmm.getClusterProbabilities(new Datum(new ArrayList<Integer>(), vectorClusterMeans.get(i)));
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
                .set(MacroBaseConf.RANDOM_SEED, 152)
                .set(MacroBaseConf.TRANSFORM_TYPE, "EM_GMM")
                .set(GMMConf.NUM_MIXTURES, 3)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(GMMConf.MAX_ITERATIONS_TO_CONVERGE, 20)
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-7000points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(7000, data.size());

        ExpectMaxGMM gmm = new ExpectMaxGMM(conf);
        gmm.train(data);

        double[][] clusterMeans = {
                {1.5, 2},
                {2, 0},
                {4.5, 1},
        };
        List<RealVector> vectorClusterMeans = new ArrayList<>(3);
        for (int k =0; k< 3; k++) {
            vectorClusterMeans.add(new ArrayRealVector(clusterMeans[k]));
        }
        double [][][] clusterCovariances = {
                {{0.5,0.4},{0.4,0.5}},
                {{0.3,0},{0,0.6}},
                {{0.9,0.2},{0.2,0.3}},
        };

        List<RealVector> calculatedMeans = gmm.getClusterCenters();
        List<RealMatrix> calculatedCovariances = gmm.getClusterCovariances();

        for (int i=0; i<3 ; i ++) {
            boolean identified = false;
            for (int j=0; j<3; j++) {
                if (calculatedMeans.get(i).getDistance(vectorClusterMeans.get(j)) < 0.1 ) {
                    for (int p=0; p<2; p++) {
                        for (int q=0; q<2; q++) {
                            assertEquals(clusterCovariances[j][p][q], calculatedCovariances.get(i).getEntry(p,q), 0.1);
                        }
                    }
                    identified = true;
                    break;
                }
            }
            assertEquals("a cluster was not identified", true, identified);
        }
    }
}

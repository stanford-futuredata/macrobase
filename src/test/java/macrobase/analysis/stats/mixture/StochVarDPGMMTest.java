package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.util.Drainer;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class StochVarDPGMMTest {

    /**
     * Tests Bayesian Gaussian Mixture Model on a three well separated clusters.
     */
    @Test
    public void bivariateWellSeparatedNormalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 48)
                .set(MacroBaseConf.TRANSFORM_TYPE, "SVI_DPGMM")
                .set(MacroBaseConf.DPM_TRUNCATING_PARAMETER, 10)
                .set(MacroBaseConf.DPM_CONCENTRATION_PARAMETER, 0.1)
                .set(MacroBaseConf.NUM_MIXTURES, 3) // Used to compare with EM algorithm
                .set(MacroBaseConf.MIXTURE_CENTERS_FILE, "src/test/resources/data/3gaussians-700.points-centers.json")
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-700points.csv.gz")
                .set(MacroBaseConf.HIGH_METRICS, "XX, YY")
                .set(MacroBaseConf.LOW_METRICS, "")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        assertEquals(700, data.size());

        StochVarDPGMM dpgmm = new StochVarDPGMM(conf);
        List<RealVector> calculatedMeans;

        // Make sure we have 3 clusters. Sometimes initialization is not great.
        dpgmm.train(data);

        calculatedMeans = dpgmm.getClusterCenters();
        List<RealMatrix> calculatedCovariances = dpgmm.getClusterCovariances();

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
            double[] probas = dpgmm.getClusterProbabilities(new Datum(new ArrayList<Integer>(), vectorClusterMeans.get(i)));
            for (int j=0; j< 3; j++) {
                maxProbas[j] = Math.max(probas[j], maxProbas[j]);
            }
        }
        for (int j=0; j< 3; j++) {
            assertEquals(maxProbas[j], 1, 0.01);
        }
    }
}

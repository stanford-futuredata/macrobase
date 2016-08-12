package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateNormal;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.util.Drainer;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;

public class VarGMMTest {

    @Test
    /**
     * Tests Gaussian Mixture Model on a three not so well separated clusters.
     */
    public void bivariateOkSeparatedNormalTest() throws Exception {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.RANDOM_SEED, 0)
                .set(MacroBaseConf.TRANSFORM_TYPE, "MEAN_FIELD_GMM")
                .set(GMMConf.NUM_MIXTURES, 3)
                .set(GMMConf.MAX_ITERATIONS_TO_CONVERGE, 15)
                .set(GMMConf.ITERATIVE_PROGRESS_CUTOFF_RATIO, 0.001)
                .set(MacroBaseConf.DATA_LOADER_TYPE, "CSV_LOADER")
                .set(GMMConf.TRAIN_TEST_SPLIT, 0.9)
                .set(MacroBaseConf.CSV_COMPRESSION, CSVIngester.Compression.GZIP)
                .set(MacroBaseConf.CSV_INPUT_FILE, "src/test/resources/data/3gaussians-7000points.csv.gz")
                .set(MacroBaseConf.METRICS, "XX, YY")
                .set(MacroBaseConf.ATTRIBUTES, "");
        List<Datum> data = Drainer.drainIngest(conf);
        int totalPoints = 7000;
        assertEquals(totalPoints, data.size());

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

        double[] clusterWeights = {
                2000,
                3000,
                2000,
        };

        FiniteGMM finiteGMM = new FiniteGMM(conf);
        finiteGMM.train(data);

        List<MultivariateNormal> normals = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            normals.add(new MultivariateNormal(vectorClusterMeans.get(i), new BlockRealMatrix(clusterCovariances[i])));
        }
        Random rand = conf.getRandom();
        for (int i = 0; i < 10; i++) {
            Datum d = data.get(rand.nextInt(totalPoints));
            double density = 0;
            for (int j = 0; j < 3; j++) {
                density += clusterWeights[j] / totalPoints * normals.get(j).density(d.metrics());
            }
            // Finite Model takes longer to converge, and since we are limiting num
            // iterations, take a conservative limit on deviation.
            assertEquals(density, Math.exp(finiteGMM.score(d)), 0.06);
        }

        double[] farPoint = {1000, 10000};
        assertEquals(finiteGMM.ZERO_LOG_SCORE, finiteGMM.score(new Datum(new ArrayList<Integer>(), new ArrayRealVector(farPoint))), 1e-9);
    }
}

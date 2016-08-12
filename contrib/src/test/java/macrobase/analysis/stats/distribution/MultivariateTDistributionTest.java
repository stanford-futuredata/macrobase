package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.linear.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertEquals;

public class MultivariateTDistributionTest {
    private static final Logger log = LoggerFactory.getLogger(MultivariateTDistributionTest.class);

    @Test
    public void testReductionToStudentT() {
        double mean = 0;
        double variance = 1;
        RealVector meanVector = new ArrayRealVector(1);
        meanVector.setEntry(0, mean);
        RealMatrix varianceMatrix = MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(variance);

        int N = 4300;
        MultivariateTDistribution multiT = new MultivariateTDistribution(meanVector, varianceMatrix, N);
        TDistribution uniT = new TDistribution(N);

        double[] testPoints = {0.5, 1, 2, 3, 15, 13.3, 17.6, 19.2, 300.2, 10.4};

        RealVector v = new ArrayRealVector(1);
        for (double x : testPoints) {
            v.setEntry(0, x);
            assertEquals(uniT.density(x), multiT.density(v), 1e-5);
        }
    }

    @Test
    public void univariateMeanTest() {
        // Test that the probability at mean is roughly 1 / sqrt(2 * variance * PI)
        double[][] meanCovN = {
                {0, 1, 100},
                {0, 1, 1000},
                {0, 1, 10000},
                {0, 1, 10001},
                {0, 1, 100001},
                {0, 1, 1000100},
                {0, 1, 10001003},
                {0, 2, 1000100},
                {0, 3, 10001003},
                {1.3, 3, 10001003},
                {1.3, 3.2, 10001003},
                {5.3, 8.2, 10001004},
        };

        MultivariateTDistribution multiT;
        RealVector v = new ArrayRealVector(1);
        for (double[] mCN : meanCovN) {
            v.setEntry(0, mCN[0]);
            double var = mCN[1];
            multiT = new MultivariateTDistribution(
                    v,
                    MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(var),
                    (int) mCN[2]);
            assertEquals(1. / Math.sqrt(2 * var * Math.PI), multiT.density(v), 1e-3);
        }
    }

    /**
     * Compare with R package mvtnorm
     */
    @Test
    public void valueTest() {
        double[][] means  = {
                {5.8, 1.3},
                {0.25, 10.96},
        };
        double[][][] covariances = {
                {{0.18, 0.03}, {0.03, 0.27}},
                {{0.55, 0.018}, {0.018, 0.21}},
        };
        double[] dofs = {
                292.1,
                10.7,
        };

        double[][][] points = {
                {{5.8, 1.3}, {5.2, 2.3}},
                {{0.25, 10.96}, {0, 10}, {1, 11}},
        };

        double[][] pdfs = {
                {0.7287204, 0.02772672},
                {0.4689636, 0.05175506, 0.2624979},
        };

        for (int s=0; s< means.length; s ++) {
            MultivariateTDistribution mvtd = new MultivariateTDistribution(new ArrayRealVector(means[s]), new BlockRealMatrix(covariances[s]), dofs[s]);
            for (int i =0; i< points[s].length; i++) {
                assertEquals(pdfs[s][i], mvtd.density(new ArrayRealVector(points[s][i])), 1e-7);
            }
        }
    }
}

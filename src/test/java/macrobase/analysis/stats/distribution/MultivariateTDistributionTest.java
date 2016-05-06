package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.special.Gamma;
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
    public void gammaRatioTest() {
        int[][] nonApproximatePairs = {
                {5, 3},
                {7, 5},
                {8, 6},
                {11, 7},
                {11, 1},
                {22, 20},
                {22, 12},
        };
        for (int[] nd : nonApproximatePairs) {
            assertEquals(Gamma.gamma(nd[0]/ 2.0) / Gamma.gamma(nd[1] / 2.0), MultivariateTDistribution.halfGammaRatio(nd[0], nd[1]), 1e-8);
            assertEquals(Gamma.gamma(nd[0]/ 2.0) / Gamma.gamma(nd[1] / 2.0), MultivariateTDistribution.halfGammaRatio((double) nd[0], (double) nd[1]), 1e-8);
        }

        int[][] approximatePairs = {
                {4, 3},
                {11, 2},
                {2, 1},
                {3, 2},
                {6, 1},
                {7, 2},
                {10, 3},
        };
        for (int[] nd : nonApproximatePairs) {
            assertEquals(Gamma.gamma(nd[0]/ 2.0) / Gamma.gamma(nd[1] / 2.0), MultivariateTDistribution.halfGammaRatio(nd[0], nd[1]), 1e-8);
            assertEquals(Gamma.gamma(nd[0]/ 2.0) / Gamma.gamma(nd[1] / 2.0), MultivariateTDistribution.halfGammaRatio((double) nd[0], (double) nd[1]), 1e-8);
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
}

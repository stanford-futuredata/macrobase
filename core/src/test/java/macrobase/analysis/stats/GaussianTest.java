package macrobase.analysis.stats;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class GaussianTest {
    private static final Logger log = LoggerFactory.getLogger(MADTest.class);

    private static MultivariateNormalDistribution getSample3dNormal() {
        double[] mean = {1,2,3};
        double[][] cov = {
                {3,1,0},
                {1,3,1},
                {0,1,3}
        };
        MultivariateNormalDistribution mvNormal = new MultivariateNormalDistribution(mean, cov);
        return mvNormal;
    }

    @Test
    public void testFitGaussian() {
        MultivariateNormalDistribution mvNormal = getSample3dNormal();

        int N = 1000000;
        int k = 3;
        List<double[]> testData = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            testData.add(mvNormal.sample());
        }

        long startTime = System.currentTimeMillis();
        Gaussian fitted = new Gaussian().fit(testData);
        long endTime = System.currentTimeMillis();
        log.debug("Fitted {} in: {}", N, endTime - startTime);
        assertArrayEquals(mvNormal.getMeans(), fitted.getMean(), 0.01);
        for (int i = 0; i < k; i++) {
            assertArrayEquals(
                    mvNormal.getCovariances().getRow(i),
                    fitted.getCovariance().getRow(i), 0.05);
        }
    }

    @Test
    public void testMahalanobis() {
        MultivariateNormalDistribution mvNormal = getSample3dNormal();
        Gaussian gaussian = new Gaussian(mvNormal.getMeans(), mvNormal.getCovariances());

        int N = 100000;
        int k = 3;
        double[][] testData = new double[N][k];
        for (int i = 0; i < N; i++) {
            testData[i] = mvNormal.sample();
        }

        double[] mScores = new double[N];
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < N; i++) {
            mScores[i] = gaussian.mahalanobis(testData[i]);
        }
        long endTime = System.currentTimeMillis();
        log.debug("Mahalobis distance on {} in {}", N, endTime-startTime);

        double[] dScores = new double[N];
        startTime = System.currentTimeMillis();
        for (int i = 0; i < N; i++) {
            dScores[i] = -Math.log(mvNormal.density(testData[i]));
        }
        endTime = System.currentTimeMillis();
        log.debug("LogPDF on {} in {}", N, endTime-startTime);

        // Check that mahalonbis distance has same relative magnitude as -log(pdf)
        for (int i = 1; i < N; i++) {
            assertEquals(mScores[i] > mScores[i-1], dScores[i] > dScores[i-1]);
        }
    }
}

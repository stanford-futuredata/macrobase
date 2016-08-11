package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class MixtureTest {
    private static final Logger log = LoggerFactory.getLogger(MixtureTest.class);
    @Test
    public void nonZeroScoreTest() {
        List<MultivariateDistribution> listDist = new ArrayList<>(3);
        double[] weights = {2. / 7, 3. / 7, 2. / 7};
        double[][] distData = {
                {1.5, 2}, {0.5, 0.4, 0.4, 0.5}, {2000},
                {2, 0}, {0.3, 0, 0, 0.6}, {3000},
                {4.5, 1}, {0.9, 0.2, 0.2, 0.3}, {2000}};
        for (int i = 0; i < distData.length; i += 3) {
            RealVector mean = new ArrayRealVector(distData[i + 0]);
            double[][] covArray = new double[2][2];
            covArray[0] = Arrays.copyOfRange(distData[i + 1], 0, 2);
            covArray[1] = Arrays.copyOfRange(distData[i + 1], 2, 4);
            RealMatrix cov = new BlockRealMatrix(covArray);
            listDist.add(new MultivariateNormal(mean, cov));
        }

        Mixture mixture = new Mixture(listDist, weights);

        assertEquals(0.155359, mixture.density(new ArrayRealVector(distData[0])), 1e-6);
        assertEquals(0.162771, mixture.density(new ArrayRealVector(distData[3])), 1e-6);
        assertEquals(0.094819, mixture.density(new ArrayRealVector(distData[6])), 1e-6);
    }
}

package macrobase.analysis.stats.kde.kernel;

import macrobase.analysis.stats.kde.kernel.EpaKernel;
import macrobase.analysis.stats.kde.kernel.GaussianKernel;
import macrobase.analysis.stats.kde.kernel.Kernel;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.DiagonalMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class KernelTest {
    @Test
    public void test1dGaussian() {
        double[] bw = new double[1];
        bw[0] = 1;

        Kernel g = new GaussianKernel().initialize(bw);

        double[] x = new double[1];
        x[0] = 0;
        assertEquals(1.0/Math.sqrt(2*Math.PI), g.density(x), 1e-10);

        x[0] = 5;
    }

    @Test
    public void testMultivariateGaussian() {
        double[] mean = {0.0, 0.0, 0.0};
        double[] bw = {1.0, 2.0, 3.0};
        double[][] cov = new DiagonalMatrix(new double[] {1.0, 4.0, 9.0}).getData();
        MultivariateNormalDistribution m = new MultivariateNormalDistribution(
                mean,
                cov
        );

        Kernel g= new GaussianKernel().initialize(bw);

        double[][] testPts = {
                {0.0,0.0,0.0},
                {1.0,-1.0,1.0},
                {-3.42,-34,0.003}
        };
        for (double[] pt: testPts) {
            double d1 = m.density(pt);
            double d2 = g.density(pt);
            assertEquals(d1, d2, 1e-10*d1);
        }
    }

    @Test
    public void testEpa() {
        double[] bw = {1.0, 1.0};
        Kernel g = new EpaKernel().initialize(bw);

        double[] x1 = {0.5, 0.5};
        double expected = .75*.75*.75*.75;
        assertEquals(expected, g.density(x1), 1e-5);

        double[] x2 = {2, 0.5};
        assertEquals(0, g.density(x2), 1e-5);

        double[] x3 = {-2, 0.5};
        assertEquals(0, g.density(x3), 1e-5);
    }

}

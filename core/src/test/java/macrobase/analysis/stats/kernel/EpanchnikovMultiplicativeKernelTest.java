package macrobase.analysis.stats.kernel;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import static org.junit.Assert.*;

public class EpanchnikovMultiplicativeKernelTest {

    final double DOUBLE_ACCURACY = 0.00000000001;

    @Test
    public void testDensity() {
        double[] vectorData = new double[1];
        double density;

        Kernel kernel = new macrobase.analysis.stats.kernel.EpanchnikovMulticativeKernel(1);
        vectorData[0] = 2.0;
        density = kernel.density(new ArrayRealVector(vectorData));
        assertEquals(density, 0.0, DOUBLE_ACCURACY);

        vectorData[0] = 1.0;
        density = kernel.density(new ArrayRealVector(vectorData));
        assertEquals(density, 0.0, DOUBLE_ACCURACY);

        vectorData[0] = 0.0;
        density = kernel.density(new ArrayRealVector(vectorData));
        assertEquals(density, 0.75, DOUBLE_ACCURACY);
    }

    @Test
    public void testNorm() {
        double[] vectorData = new double[3];
        double norm;
        double density;

        Kernel kernel = new macrobase.analysis.stats.kernel.EpanchnikovMulticativeKernel(3);
        density = kernel.density(new ArrayRealVector(vectorData));
        assertEquals(density, 0.421875, DOUBLE_ACCURACY);

        norm = kernel.norm();
        assertEquals(norm, 0.216, DOUBLE_ACCURACY);
    }
}

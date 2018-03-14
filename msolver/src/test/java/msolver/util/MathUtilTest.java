package msolver.util;

import msolver.data.HepData;
import msolver.data.MomentData;
import msolver.data.OccupancyData;
import msolver.util.MathUtil;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MathUtilTest {
    @Test
    public void testBinomial() {
        long[][] binoms = MathUtil.getBinomials(5);
        assertEquals(binoms[5][2], 10L);
    }

    @Test
    public void testChebyCoefficient() {
        int[][] cCoeffs = MathUtil.getChebyCoefficients(5);
        int[] expected = {0, -3, 0, 4, 0, 0};
        assertArrayEquals(expected, cCoeffs[3]);
    }

    @Test
    public void testConvertMoments() {
        // integers from 0...1000
        double[] uniformPowerSums = {1001,500500,333833500,250500250000L};
        double[] convertedChebyshevMoments = MathUtil.powerSumsToChebyMoments(0, 1000, uniformPowerSums);

        double[] expectedChebyshevMoments = {1.0, 0, -.332, 0};
        assertArrayEquals(expectedChebyshevMoments, convertedChebyshevMoments, 1e-14);
    }

    @Test
    public void testChebyAccuracy() {
        MomentData data = new OccupancyData();
        double[] chebys = MathUtil.powerSumsToChebyMoments(
                data.getMin(), data.getMax(),
                data.getPowerSums(20)
        );
//        chebys = MathUtil.powerSumsToChebyMoments(
//                data.getLogMin(), data.getLogMax(),
//                data.getLogSums(20)
//        );
        data = new HepData();
        chebys = MathUtil.powerSumsToChebyMoments(
                data.getMin(), data.getMax(),
                data.getPowerSums(20)
        );
    }
}
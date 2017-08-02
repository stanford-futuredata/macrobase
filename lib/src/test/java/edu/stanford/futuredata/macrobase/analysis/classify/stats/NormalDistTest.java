package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import org.junit.Test;

import static org.junit.Assert.*;
import org.apache.commons.math3.distribution.NormalDistribution;

public class NormalDistTest {
    @Test
    public void testCDF() throws Exception {
        NormalDist dist = new NormalDist();
        double cdf;

        cdf = dist.cdf(0.0, 1.0, 0.5);
        assertEquals(0.6915, cdf, 0.001);

        cdf = dist.cdf(5.0, 2.0, 4.0);
        assertEquals(0.3085, cdf, 0.001);

        // Test interpolation
        cdf = dist.cdf(0.0, 1.0, 0.755);
        assertEquals(0.7749, cdf, 0.001);

        // Test max z-score
        cdf = dist.cdf(0.0, 1.0, 4.0);
        assertEquals(1.0, cdf, 0.001);

        // Test min z-score
        cdf = dist.cdf(0.0, 1.0, -4.0);
        assertEquals(0.0, cdf, 0.001);
    }

    @Test
    public void checkLUT() {
        int numElements =
                (int)((NormalDist.MAXZSCORE - NormalDist.MINZSCORE)/NormalDist.GRANULARITY) + 1;
        double[] LUT = new double[numElements];
        NormalDistribution dist = new NormalDistribution();
        int minKey = (int) Math.round(NormalDist.MINZSCORE / NormalDist.GRANULARITY);
        int maxKey = (int) Math.round(NormalDist.MAXZSCORE / NormalDist.GRANULARITY);
        int offset = -minKey;
        for (int i = minKey; i <= maxKey; i ++) {
            double zscore = i * NormalDist.GRANULARITY;
            LUT[i+offset] = dist.cumulativeProbability(zscore);
        }

        assertEquals(NormalDist.CDF_LUT.length, LUT.length);
        for (int i = 0; i < LUT.length; i++) {
            assertEquals(NormalDist.CDF_LUT[i], LUT[i], 0.001);
        }
    }
}
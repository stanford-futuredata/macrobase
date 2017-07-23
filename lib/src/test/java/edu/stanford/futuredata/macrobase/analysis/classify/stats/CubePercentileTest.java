package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CubePercentileTest {
    private double[] counts;
    private double[] metrics;

    @Before
    public void setUp() {
        int length = 1000;
        counts = new double[length];
        metrics = new double[length];
        for (int i = 0; i < length; i++) {
            counts[i] = i;
            metrics[i] = i;
        }
    }

    @Test
    public void testEvaluate() throws Exception {
        CubePercentile cp = new CubePercentile(counts, metrics);

        // p100 is defined as the largest element
        double p100 = cp.evaluate(100.0);
        assertEquals(999.0, p100, 0.01);

        double p99 = cp.evaluate(99.0);
        assertEquals(994.0, p99, 0.01);

        double p90 = cp.evaluate(90.0);
        assertEquals(948.0, p90, 0.01);

        double p50 = cp.evaluate(50.0);
        assertEquals(707.0, p50, 0.01);

        double p1 = cp.evaluate(1.0);
        assertEquals(100.0, p1, 0.01);

        double p0 = cp.evaluate(0.0);
        assertEquals(1.0, p0, 0.01);
    }

}
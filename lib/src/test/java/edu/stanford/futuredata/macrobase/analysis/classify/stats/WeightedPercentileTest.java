package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class WeightedPercentileTest {
    private double[] counts;
    private double[] metrics;

    @Before
    public void setUp() {
        int length = 1000;
        counts = new double[length];
        metrics = new double[length];
        for (int i = 0; i < length; i++) {
            counts[i] = i+1;
            metrics[i] = i+1;
        }
    }

    @Test
    public void testEvaluate() throws Exception {
        WeightedPercentile wp = new WeightedPercentile(counts, metrics);

        // p100 is defined as the largest element
        double p100 = wp.evaluate(100.0);
        assertEquals(1000.0, p100, 0.01);

        double p99 = wp.evaluate(99.0);
        assertEquals(995.0, p99, 0.01);

        double p90 = wp.evaluate(90.0);
        assertEquals(949.0, p90, 0.01);

        double p50 = wp.evaluate(50.0);
        assertEquals(707.0, p50, 0.01);

        double p1 = wp.evaluate(1.0);
        assertEquals(100.0, p1, 0.01);

        double p0 = wp.evaluate(0.0);
        assertEquals(1.0, p0, 0.01);
    }

}
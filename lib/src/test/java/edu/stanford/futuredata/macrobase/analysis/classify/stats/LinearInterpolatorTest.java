package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LinearInterpolatorTest {
    private double[] x;
    private double[] y;

    @Before
    public void setUp() {
        x = new double[]{-2.0, 0.0, 1.0, 1.0, 3.0};
        y = new double[]{1.0, 3.0, 3.0, 7.0, -1.0};
    }

    @Test
    public void testEvaluate() throws Exception {
        LinearInterpolator interpolator = new LinearInterpolator(x, y);
        double result;

        result = interpolator.evaluate(-3);
        assertEquals(Double.NaN, result, 0.01);

        result = interpolator.evaluate(-2);
        assertEquals(1.0, result, 0.01);

        result = interpolator.evaluate(-1);
        assertEquals(2.0, result, 0.01);

        result = interpolator.evaluate(0);
        assertEquals(3.0, result, 0.01);

        result = interpolator.evaluate(0.5);
        assertEquals(3.0, result, 0.01);

        result = interpolator.evaluate(1);
        assertEquals(3.0, result, 0.01);

        result = interpolator.evaluate(1.5);
        assertEquals(5.0, result, 0.01);

        result = interpolator.evaluate(3);
        assertEquals(-1.0, result, 0.01);

        result = interpolator.evaluate(3.5);
        assertEquals(Double.NaN, result, 0.01);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDimensionMismatch() throws Exception {
        double[] x = new double[]{1, 2, 3};
        double[] y = new double[]{1, 2};
        LinearInterpolator interpolator = new LinearInterpolator(x, y);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNumberIsTooSmall() throws Exception {
        double[] x = new double[]{1};
        double[] y = new double[]{1};
        LinearInterpolator interpolator = new LinearInterpolator(x, y);
    }
}
package edu.stanford.futuredata.macrobase.analysis;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class PercentileFucntionTest {

    private double[] input;
    private double[] expectedOutput;

    @Before
    public void setUp() throws Exception {
        input = new double[]{0.1, 0.3, 0.2, 0.5, 0.4};
        expectedOutput = new double[]{0.2, 0.6, 0.4, 1.0, 0.8};
    }

    @Test
    public void simpleTest() throws MacroBaseException {
        MBFunction func = MBFunction.getFunction("percentile", "");
        final double[] output = new double[input.length];
        func.applyFunction(input, output);
        assertTrue(Arrays.equals(output, expectedOutput));
    }

}
package msolver;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class MnatSolverTest {
    @Test
    public void testUniform() {
        double[] m_values = {1.0, 1.0/2, 1.0/3, 1.0/4, 1.0/5, 1.0/6, 1.0/7};

        double[] cdf = MnatSolver.estimateCDF(m_values);
        double[] qs = MnatSolver.estimateQuantiles(0, 1, m_values, Arrays.asList(.2, .5, .8));
        double[] expectedQs = {.2, .5, .8};
        assertArrayEquals(expectedQs, qs, .1);
    }
}
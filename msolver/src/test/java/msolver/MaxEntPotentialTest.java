package msolver;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MaxEntPotentialTest {
    @Test
    public void testTrivial() {
        double m_values[] = {1.0, 0, -1.0/3, 0, -1.0/15, 0, -1.0/35};
        double l_values[] = {0.0, 0, 0, 0, 0, 0, 0};
        double tol = 1e-10;
        MaxEntPotential P = new MaxEntPotential(m_values);
        P.computeAll(l_values, tol);
        assertEquals(m_values.length, P.getGradient().length);
    }
}
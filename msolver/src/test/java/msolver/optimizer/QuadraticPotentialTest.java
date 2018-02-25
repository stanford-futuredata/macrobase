package msolver.optimizer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QuadraticPotentialTest {
    @Test
    public void testSimple() {
        QuadraticPotential qp = new QuadraticPotential(2);
        double[] x = {1.0, 2.0};
        qp.computeAll(x, 0.0);
        assertEquals(5, qp.getValue(), 1e-10);

        double[] xMin = {0.0, 0.0};
        qp.computeAll(xMin, 0.0);
        assertEquals(0, qp.getGradient()[1], 1e-10);
    }

}
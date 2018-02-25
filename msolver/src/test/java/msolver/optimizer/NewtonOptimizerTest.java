package msolver.optimizer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NewtonOptimizerTest {
    @Test
    public void testQuadratic() {
        QuadraticPotential qp = new QuadraticPotential(2);
        NewtonOptimizer opt = new NewtonOptimizer(qp);
        double[] start = {1.0, 2.0};
        double[] solution = opt.solve(start, 1e-10);
        for (int i = 0; i < start.length; i++) {
            assertEquals(0.0, solution[i], 1e-10);
        }
        assertEquals(1, opt.getStepCount());
        assertEquals(0, opt.getDampedStepCount());
        assertTrue(opt.isConverged());
    }
}
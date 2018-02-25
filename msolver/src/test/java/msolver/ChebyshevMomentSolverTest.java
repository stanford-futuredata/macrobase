package msolver;

import msolver.data.RetailData;
import msolver.data.ShuttleData;
import msolver.optimizer.NewtonOptimizer;
import org.apache.commons.math3.util.FastMath;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChebyshevMomentSolverTest {
    @Test
    public void testUniform() {
        double m_values[] = {1.0, 0, -1.0/3, 0, -1.0/15, 0, -1.0/35};
        double tol = 1e-10;
        ChebyshevMomentSolver solver = new ChebyshevMomentSolver(m_values);
        solver.solve(tol);

        double[] coeffs = solver.getLambdas();

        assertEquals(FastMath.log(2), coeffs[0], 1e-10);
        for (int i = 1; i < coeffs.length; i++) {
            assertEquals(0.0, coeffs[i], 1e-10);
        }
        NewtonOptimizer opt = solver.getOptimizer();
        assertTrue(opt.getStepCount() < 20);
        assertEquals(0, opt.getDampedStepCount());
    }

    @Test
    public void testShuttle() {
        int k = 11;
        double[] m_values = new double[k];
        for (int i = 0; i < k; i++) {
            m_values[i] = ShuttleData.moments[i];
        }
        double tol = 1e-10;
        ChebyshevMomentSolver solver = new ChebyshevMomentSolver(m_values);
        solver.solve(tol);

        double[] coeffs = solver.getLambdas();
        MaxEntFunction f = new MaxEntFunction(coeffs);
        double[] f_mus = f.moments(k, tol);
        for (int i = 0; i < k; i++) {
            assertEquals(m_values[i], f_mus[i], 10*tol);
        }
        NewtonOptimizer opt = solver.getOptimizer();
        assertTrue(opt.getStepCount() < 20);

        double[] ps = {.5};
        double[] expectedQs = {-0.602};
        assertArrayEquals(
                expectedQs,
                solver.estimateQuantiles(ps, -1, 1),
                1e-3
        );

        expectedQs[0] = 45.0;
        assertArrayEquals(
                expectedQs,
                solver.estimateQuantiles(ps, 27, 126),
                5.0
        );
    }

    @Test
    public void testRetail() {
        int k = 11;
        double[] m_values = new double[k];
        for (int i = 0; i < k; i++) {
            m_values[i] = RetailData.moments[i];
        }
        double tol = 1e-10;
        ChebyshevMomentSolver solver = new ChebyshevMomentSolver(m_values);
        solver.solve(tol);

        double[] coeffs = solver.getLambdas();
        MaxEntFunction f = new MaxEntFunction(coeffs);
        double[] f_mus = f.moments(k, tol);
        for (int i = 0; i < k; i++) {
            assertEquals(m_values[i], f_mus[i], 10*tol);
        }

        NewtonOptimizer opt = solver.getOptimizer();
        assertTrue(opt.getStepCount() < 100);
        assertTrue(opt.getDampedStepCount() > 0);
    }

}
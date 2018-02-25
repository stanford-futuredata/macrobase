package msolver;

import msolver.data.MomentData;
import msolver.data.RetailQuantityData;
import msolver.data.RetailQuantityLogData;
import msolver.data.ShuttleData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleBoundSolverTest {
    @Test
    public void testUniform() {
        // 101 evenly spaced points between 0 and 1, inclusive
        double[] values = new double[101];
        for (int i = 0; i <= 100; i++) {
            values[i] = 0.01 * i;
        }

        int k = 7;
        double m_values[] = new double[k];
        for (int i = 0; i < k; i++) {
            double moment = 0.0;
            for (double v : values) {
                moment += Math.pow(v, i);
            }
            m_values[i] = moment;
        }
        double[] moments = MathUtil.powerSumsToMoments(m_values);

        SimpleBoundSolver solver = new SimpleBoundSolver(k);
        double[] xs = {.5, .6};
        double[] ps = {.5, .6};
        double[] boundSizes = solver.solveBounds(moments, xs);
        assertEquals(.4444, boundSizes[0], 1e-4);
        double[] maxErrors = solver.getMaxErrors(moments, xs, ps, boundSizes);
        assertEquals(.2222, maxErrors[0], 1e-4);
        assertTrue(maxErrors[1] < .2222);
    }

    @Test
    public void testShuttle() {
        int k = 11;
        double[] m_values = new double[k];
        for (int i = 0; i < k; i++) {
            m_values[i] = ShuttleData.powerSums[i];
        }
        double[] moments = MathUtil.powerSumsToMoments(m_values);

        SimpleBoundSolver solver = new SimpleBoundSolver(k);
        double[] xs = {45.0};
        double[] ps = {.5};
        double[] boundSizes = solver.solveBounds(moments, xs);
        double[] maxError = solver.getMaxErrors(moments, xs, ps, boundSizes);
        assertEquals(.2, maxError[0], 0.01);
    }

//    @Test
//    public void testBoundsDecrease() {
//        double prevQError = 1.0;
//        for (int k = 3; k <= 11; k++) {
//            double[] m_values = new double[k];
//            for (int i = 0; i < k; i++) {
//                m_values[i] = ShuttleData.powerSums[i];
//            }
//
//            SimpleBoundSolver solver = new SimpleBoundSolver(k);
//            solver.solveBounds(m_values, ShuttleData.min, ShuttleData.max, Arrays.asList(45.0));
//            double[] maxError = solver.getMaxErrors(Arrays.asList(.5));
//            double error = maxError[0];
//            assertTrue(error <= prevQError);
//            prevQError = error;
//        }
//    }
//
//    @Test
//    public void testMarkov() {
//        int k = 2;
//        double[] m_values = new double[k];
//        for (int i = 0; i < k; i++) {
//            m_values[i] = ShuttleData.powerSums[i];
//        }
//
//        SimpleBoundSolver solver = new SimpleBoundSolver(k);
//        solver.solveBounds(m_values, ShuttleData.min, ShuttleData.max, Arrays.asList(100.0));
//        double[] maxError = solver.getMaxErrors(Arrays.asList(.8));
//        double error = maxError[0];
//        assertEquals(.2, error, 0.01);
//    }
//
//    @Test
//    public void testRetail() {
//        int k = 11;
//        double[] m_values = new double[k];
//        for (int i = 0; i < k; i++) {
//            m_values[i] = RetailData.powerSums[i];
//        }
//
//        SimpleBoundSolver solver = new SimpleBoundSolver(k);
//        solver.solveBounds(m_values, RetailData.min, RetailData.max, Arrays.asList(45.0));
//        double[] maxError = solver.getMaxErrors(Arrays.asList(.9));
//        double error = maxError[0];
//        assertEquals(0.14, error, 0.01);
//    }

    @Test
    public void testCanonical() {
        int k = 7;
        List<MomentData> dataSets = Arrays.asList(
                new RetailQuantityData(),
                new RetailQuantityLogData()
        );

        List<Double> entropies = new ArrayList<>(2);
        for (MomentData data : dataSets) {
            double[] powerSums = data.getPowerSums(k);
            double min = data.getMin();
            double max = data.getMax();
            double[] moments = MathUtil.powerSumsToPosMoments(powerSums, min, max);
            SimpleBoundSolver solver = new SimpleBoundSolver(k);
            double[] xs = {0, 1};
            SimpleBoundSolver.CanonicalDistribution[] sols = solver.getCanonicalDistributions(moments, xs);
            entropies.add(sols[0].entropy()+sols[1].entropy());
        }
        assertTrue(entropies.get(0) < entropies.get(1));
        assertTrue(entropies.get(1) > 0);
    }
}
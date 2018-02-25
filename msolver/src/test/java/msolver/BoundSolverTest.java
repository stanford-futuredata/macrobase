package msolver;

import msolver.data.RetailData;
import msolver.data.ShuttleData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoundSolverTest {
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

        BoundSolver solver = new BoundSolver(m_values, 0, 1);
        double error = solver.quantileError(0.5, 0.5);
        assertEquals(error, 0.22, 0.01);
    }

    @Test
    public void testShuttle() {
        int k = 11;
        double[] m_values = new double[k];
        for (int i = 0; i < k; i++) {
            m_values[i] = ShuttleData.powerSums[i];
        }

        BoundSolver solver = new BoundSolver(m_values, ShuttleData.min, ShuttleData.max);
        double error = solver.quantileError(45, 0.5);
        assertEquals(error, 0.2, 0.01);

        double boundSizeRacz = solver.boundSizeRacz(45);
        double boundSizeLindsay = solver.boundSizeLindsay(45);
        assertEquals(boundSizeRacz, boundSizeLindsay, 1e-4);
    }

    @Test
    public void testBoundsDecrease() {
        double prevQError = 1.0;
        for (int k = 3; k <= 11; k++) {
            double[] m_values = new double[k];
            for (int i = 0; i < k; i++) {
                m_values[i] = ShuttleData.powerSums[i];
            }

            BoundSolver solver = new BoundSolver(m_values, ShuttleData.min, ShuttleData.max);
            double error = solver.quantileError(45, 0.5);
            assertTrue(error <= prevQError);
            prevQError = error;
        }
    }

    @Test
    public void testMarkov() {
        int k = 2;
        double[] m_values = new double[k];
        for (int i = 0; i < k; i++) {
            m_values[i] = ShuttleData.powerSums[i];
        }

        BoundSolver solver = new BoundSolver(m_values, ShuttleData.min, ShuttleData.max);
        double error = solver.quantileError(100, 0.8);
        assertEquals(error, 0.2, 0.01);
    }

    @Test
    public void testRetail() {
        int k = 11;
        double[] m_values = new double[k];
        for (int i = 0; i < k; i++) {
            m_values[i] = RetailData.powerSums[i];
        }

        BoundSolver solver = new BoundSolver(m_values, RetailData.min, RetailData.max);
        double error = solver.quantileError(45, 0.9);
        assertEquals(error, 0.14, 0.01);
    }


    /* Experimental tests */

//    @Test
//    public void testGoriaBounds() throws Exception {
//
//        DataSource source = new SimpleCSVDataSource("src/test/resources/occupancy_2.csv", 3);
//        double[] data = source.get();
//        Arrays.sort(data);
//        List<Double> quantiles = Arrays.asList(0.1, 0.5, 0.9);
//
//        MomentSketch sketch = new MomentSketch(1e-10);
//        sketch.setCalcError(true);
//        sketch.setSizeParam(13);
//        sketch.initialize();
//        sketch.add(data);
//        double[] qs = sketch.getQuantiles(quantiles);
//
//        BoundSolver boundSolver = new BoundSolver(sketch.getPowerSums(), sketch.getMin(), sketch.getMax());
//        double goriaBound = boundSolver.quantileErrorGoria();
//        System.out.println(goriaBound);
//        boundSolver.printEntropies();
//
//        for (int i = 0; i < quantiles.size(); i++) {
//            double trueQuantile = quantiles.get(i);
//            double quantileOfEst = quantileOfValue(data, qs[i]);
//            System.out.println(qs[i]);
//            assertEquals(trueQuantile, quantileOfEst, goriaBound);
//        }
//    }
//
//    private double quantileOfValue(double[] data, double value) {
//        int i = 0;
//        int j = data.length-1;
//        while(i <= j){
//            int mid = (i+j)/2;
//            if (value > data[mid]){
//                i = mid+1;
//            } else if (value < data[mid]){
//                j = mid-1;
//            } else{
//                return mid;
//            }
//        }
//        return (double)i / data.length;
//    }

//    @Test
//    public void testNoSingularMatrixIssues() throws Exception {
//        testCustom("src/test/resources/shuttle.csv", 0, 13);
//        testCustom("src/test/resources/norm_outlier.csv", 0, 11);
//    }
//
//    private void testCustom(String filename, int column, int k) throws Exception {
//        DataSource source = new SimpleCSVDataSource(filename, column);
//        double[] data = source.get();
//        Arrays.sort(data);
//        List<Double> quantiles = Arrays.asList(
//                0.01, 0.05, 0.1, 0.15,
//                0.2, 0.25 ,0.3, 0.35,
//                0.4, 0.45, 0.5, 0.55,
//                0.6, 0.65, 0.7, 0.75,
//                0.8, 0.85, 0.9, 0.95,
//                0.99);
//
//        MomentSketch sketch = new MomentSketch(1e-10);
//        sketch.setCalcError(true);
//        sketch.setSizeParam(k);
//        sketch.initialize();
//        sketch.add(data);
//        double[] qs = sketch.getQuantiles(quantiles);
//    }
}
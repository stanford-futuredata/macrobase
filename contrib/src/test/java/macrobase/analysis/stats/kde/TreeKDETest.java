package macrobase.analysis.stats.kde;

import macrobase.util.data.TinyDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TreeKDETest {
    public static List<double[]> tinyData;

    @BeforeClass
    public static void setUp() {
        tinyData = new TinyDataSource().get();
    }


    @Test
    public void simpleTest() throws Exception {
        List<double[]> data = tinyData;

        KDTree tree = new KDTree().setLeafCapacity(3);
        TreeKDE kde = new TreeKDE(tree).setTolerance(0.0);
        kde.train(data);

        SimpleKDE simpleKDE = new SimpleKDE();
        simpleKDE.train(data);

        assertArrayEquals(simpleKDE.getBandwidth(), kde.getBandwidth(), 1e-10);
        for (double[] datum : data) {
            double dSimple = simpleKDE.density(datum);
            double dTree = kde.density(datum);
            assertEquals(dSimple, dTree, dSimple*1e-10);
        }
    }

    private void approxTest(
            List<double[]> data,
            double tol,
            double cutoffH,
            boolean ignoreSelf,
            boolean splitByWidth
    ) {
        KDTree tree = new KDTree()
                .setLeafCapacity(3)
                .setSplitByWidth(splitByWidth)
                ;
        TreeKDE kde = new TreeKDE(tree)
                .setTolerance(tol)
                .setCutoffH(cutoffH)
                .setIgnoreSelf(ignoreSelf);
        kde.train(data);

        double checkTol = Math.max(tol, 1e-12);

        SimpleKDE simpleKDE = new SimpleKDE();
        simpleKDE.setBandwidth(kde.getBandwidth())
                .train(data)
                .setIgnoreSelf(ignoreSelf);
        for (int i=0;i<data.size();i++) {
            double[] d = data.get(i);

            double trueDensity = simpleKDE.density(d);
            double estDensity = kde.density(d);
            System.out.println(trueDensity+" "+estDensity);

            if (trueDensity < cutoffH) {
                assertEquals(trueDensity, estDensity, checkTol);
            }
        }
    }

    @Test
    public void testTwoPointIgnoreSelf() {
        List<double[]> data = new ArrayList<>();
        data.add(new double[] {0.0, 0.0});
        data.add(new double[] {1.0, 1.0});

        approxTest(data, 0.0, Double.MAX_VALUE, true, true);
    }

    @Test
    public void testIgnoreSelfExact() throws Exception {
        List<double[]> data = tinyData;
        approxTest(data, 0.0, Double.MAX_VALUE, true, true);
    }

    @Test
    public void testExactSplitMedian() throws Exception {
        List<double[]> data = tinyData;
        approxTest(data, 0.0, Double.MAX_VALUE, false, false);
    }

    @Test
    public void testTolerance() throws Exception {
        List<double[]> data = tinyData;
        approxTest(data, 1e-5, 0.0, false, true);
    }

    @Test
    public void testCutoff() throws Exception {
        List<double[]> data = tinyData;
        approxTest(data, 0.0, 7e-4, false, true);
    }

    @Test
    public void testToleranceCutoff() throws Exception {
        List<double[]> data = tinyData;
        approxTest(data, 1e-5, 7e-4, false, true);
    }

    @Test
    public void testIgnoreSelfApprox() throws Exception {
        List<double[]> data = tinyData;
        approxTest(data, 1e-7, 6e-4, true, true);
    }
}

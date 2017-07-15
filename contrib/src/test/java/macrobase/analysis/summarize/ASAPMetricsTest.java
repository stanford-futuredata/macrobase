package macrobase.analysis.summarize;


import macrobase.analysis.summarize.util.ASAPMetrics;
import macrobase.datamodel.Datum;
import macrobase.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class ASAPMetricsTest {
    private static ASAPMetrics metrics = new ASAPMetrics();

    private static List<Datum> straight = new ArrayList<>();
    private static List<Datum> wiggle = new ArrayList<>();
    private static List<Datum> normal = new ArrayList<>();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Random rand = new Random();
        Datum d;
        for (int i = 0; i < 10000; ++i) {
            d = TestUtils.createTimeDatum(i, i);
            straight.add(d);
            d = TestUtils.createTimeDatum(i, i % 2);
            wiggle.add(d);
            d = TestUtils.createTimeDatum(i, rand.nextGaussian());
            normal.add(d);
        }
    }

    @Test
    public void testKurtosis() {
        assert(Math.abs(metrics.kurtosis(straight) - -1.2) < 1e-4);
        assert(Math.abs(metrics.kurtosis(wiggle) - -2) < 1e-3);
        assert(Math.abs(metrics.kurtosis(normal)) < 0.1);
    }

    @Test
    public void testRoughness() {
        assert(Math.abs(metrics.roughness(straight)) < 1e-4);
        assert(Math.abs(metrics.roughness(wiggle) - 1) < 1e-4);
        assert(Math.abs(metrics.roughness(normal) - 1.4) < 0.1);
    }

}

package msolver.thresholds;

import msolver.data.ExponentialData;
import msolver.data.MomentData;
import msolver.struct.MomentStruct;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class RTTThresholdTest {
    @Test
    public void testSimple() {
        MomentData data = new ExponentialData();
        MomentStruct m = new MomentStruct();
        m.min = data.getMin();
        m.max = data.getMax();
        m.logMin = data.getLogMin();
        m.logMax = data.getLogMax();
        m.powerSums = data.getPowerSums(10);
        m.logSums = data.getLogSums(10);

        MomentThreshold mt = new RTTThreshold(m);
        double[] bounds, bounds2;
        bounds = mt.bound(.1);
        assertTrue(bounds[0] > 0);
        assertTrue(bounds[0] < .9);
        assertTrue(bounds[1] > .9);
        assertTrue(bounds[1] <= 1);

        MomentThreshold markov = new MarkovThreshold(m);
        bounds = mt.bound(5);
        bounds2 = markov.bound(5);
        assertTrue(bounds[0] > bounds2[0]);
        assertTrue(bounds[1] < bounds2[1]);
        assertTrue(bounds[0] < 0.01);
        assertTrue(bounds[1] > 0.01);
        assertTrue(bounds[1] <= 0.5);
    }

}
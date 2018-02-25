package msolver;

import msolver.data.ExponentialData;
import msolver.data.MomentData;
import msolver.struct.MomentStruct;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MomentSolverBuilderTest {
    @Test
    public void testSimple() {
        MomentData data = new ExponentialData();
        MomentStruct m = new MomentStruct();
        m.min = data.getMin();
        m.max = data.getMax();
        m.powerSums = data.getPowerSums(10);

        MomentSolverBuilder builder = new MomentSolverBuilder(m);
        builder.setVerbose(false);
        builder.initialize();

        boolean flag;
        flag = builder.checkThreshold(2, .01);
        assertTrue(flag);
        flag = builder.checkThreshold(4, .01);
        assertTrue(flag);

        double[] ps = {0.1, 0.5, 0.9};
        double[] qs = builder.getQuantiles(ps);
        assertEquals(0.693, qs[1], 0.001);
    }
}
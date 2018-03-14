package msolver.struct;

import msolver.PMomentSolverBuilder;
import org.junit.Test;

import static org.junit.Assert.*;

public class ArcSinhMomentStructTest {
    @Test
    public void testMerge() {
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(10);
        ArcSinhMomentStruct ms1 = new ArcSinhMomentStruct(10);
        ArcSinhMomentStruct ms2 = new ArcSinhMomentStruct(10);

        int n = 10;
        double[] xs = new double[n];
        for (int i = 0; i < xs.length; i++) {
            xs[i] = i;
        }
        ms1.add(xs);
        for (int i = 0; i < xs.length; i++) {
            xs[i] = n+i;
        }
        ms2.add(xs);


        ms.merge(ms1);
        ms.merge(ms2);
        assertEquals(0, ms.min, 1e-10);
        assertEquals(2*n, ms.powerSums[0], 1e-10);

        PMomentSolverBuilder msolver = new PMomentSolverBuilder(ms);
        double q = msolver.getQuantile(.50);
        assertEquals(9.5, q, .5);
    }
}
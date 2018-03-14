package msolver.util;

import org.apache.commons.math3.transform.DctNormalization;
import org.apache.commons.math3.transform.FastCosineTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.jtransforms.dct.DoubleDCT_1D;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class FastDCTTest {
    @Test
    public void testDCT2() {
        int n = 64;
        double[] x = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = i;
        }
        DoubleDCT_1D dct = new DoubleDCT_1D(n);
        dct.forward(x, false);
    }

    @Test
    public void testSimple() {
        int n = 65;
        double[] x = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = i;
        }

        double[] cs = new double[1];

        FastCosineTransformer t = new FastCosineTransformer(
                DctNormalization.STANDARD_DCT_I
        );
        int numIters = 10000;
        long startTime, elapsed;
        startTime = System.nanoTime();
        for (int i = 0; i < numIters; i++) {
            cs = t.transform(x, TransformType.FORWARD);
        }
        elapsed = System.nanoTime() - startTime;
//        System.out.println(Arrays.toString(cs));
//        System.out.println(elapsed);

        FastDCT dct = new FastDCT(n);
        startTime = System.nanoTime();
        for (int i = 0; i < numIters; i++) {
            cs = dct.dct(x);
        }
        elapsed = System.nanoTime() - startTime;
//        System.out.println(Arrays.toString(cs));
    }

}
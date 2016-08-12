package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class TruncateTest {

    @Test
    public void testTruncate() throws Exception {
        int k = 5;
        int index = 0;
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TRUNCATE_K, k);
        double[][] test_input = {
                {9.61, 0.42, 8.89, 5.9, 6.05, -3.98, 3.74, 6.23, 5.15, -2.51, 7.74, 8.43, 7.82, 0.25, 5.82, -1.21, 3.29, 4.05, -5.72, 7.45},
                {-2.97, 4.28, -7.08, 3.76, -2.59, 8.03, 6.91, 7.24, 0.81, 0.14, 4.06, -0.74, 5.23, 7.66, 7.35, -5.29, 8.79, 5.37, 1.41, 2.25},
                {5.75, 7.75, 3.6, -5.07, 5.26, 2.42, 5.55, 3.66, 8.07, -8.08, 1.43, 7.99, 5.84, 8.64, 0.26, 2.38, 6.31, 0.02, -6.76, -9.28},
                {2.83, 7.4, 1.71, -8.27, 3.85, 2.59, 3.73, 4.56, 7.62, 8.71, 5.81, 7.66, -1.73, 8.45, 6.41, 9.6, -1.18, 4.33, 4.79, 5.8},
                {9.62, -5.53, 9.29, 8.2, 8.59, 9.24, -7.38, 0.51, 7.35, 6.45, 1.57, -4.01, 1.19, -7.25, 4.24, 8.2, 7.18, 1.69, 1.5, -6.21}
        };
        double[][] expected_return = {
                {9.61, 0.42, 8.89, 5.9, 6.05},
                {-2.97, 4.28, -7.08, 3.76, -2.59},
                {5.75, 7.75, 3.6, -5.07, 5.26},
                {2.83, 7.4, 1.71, -8.27, 3.85},
                {9.62, -5.53, 9.29, 8.2, 8.59}
        };

        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 5; i++){
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(test_input[i]));
            data.add(d);
        }

        Truncate t = new Truncate(conf);
        t.consume(data);
        List<Datum> transformed = t.getStream().drain();
        for (Datum td: transformed) {
            double[] val = td.metrics().toArray();
            assertArrayEquals(expected_return[index++], val,0);
        }

        //testing coverage weirdness
        t.initialize();
        t.shutdown();
    }
}

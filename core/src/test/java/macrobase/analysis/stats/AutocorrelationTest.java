package macrobase.analysis.stats;


import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

public class AutocorrelationTest {

    @Test
    public void testACF() throws Exception {
        int n = 1000;
        double[] test_input = new double[n];
        for (int i = 0; i < n; i ++) {
            if (i % 2 == 0) {
                test_input[i] = 1;
            } else {
                test_input[i] = -1;
            }
        }
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < n; i ++) {
            double[] value = new double[1];
            value[0] = test_input[i];
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(value));
            data.add(d);
        }

        Autocorrelation acf = new Autocorrelation(10, 0);
        acf.evaluate(data);
        double[] expected_return = {0, -0.999, 0.998, -0.997, 0.996, -0.995, 0.994, -0.993, 0.992, -0.991};
        assertArrayEquals(expected_return, acf.correlations, 1e-5);
    }

    @Test
    public void testFindPeak() throws Exception {
        double[] test_correlations = {0, 0.9, 0.6, 0.7, 0.8, 0.5, 0, 0.1, 0.2, -0.5, -0.1, 0, -0.5, 0.8, 0, 0.8, 0.7, 0.6, 0.5, 0.4, 0.5};
        Autocorrelation acf = new Autocorrelation(test_correlations.length, 0);
        acf.correlations = new double[test_correlations.length];
        for (int i = 0; i < test_correlations.length; i ++) { acf.correlations[i] = test_correlations[i]; }
        List<Integer> peaks = acf.findPeaks();
        int[] expected_peaks = {4, 13, 15};
        assertEquals(expected_peaks.length, peaks.size());
        for (int i = 0; i < peaks.size(); i ++) {
            assertTrue(expected_peaks[i] == peaks.get(i));
        }
    }

}

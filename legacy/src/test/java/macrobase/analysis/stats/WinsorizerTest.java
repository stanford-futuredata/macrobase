package macrobase.analysis.stats;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class WinsorizerTest {
    @Test
    public void testTrimOutlier() {
        int n = 10;
        int k = 2;
        List<double[]> metrics = new ArrayList<double[]>(n);
        for (int i = 0; i < n; i++) {
            double[] curVec = new double[k];
            for (int j = 0; j < k; j++) {
                curVec[j] = (double)i;
            }
            metrics.add(curVec);
        }
        // Add an outlier here
        metrics.get(5)[0] = 1e5;
        metrics.get(5)[1] = -0.24;

        Winsorizer trimmer = new Winsorizer(20);
        List<double[]> trimmed = trimmer.process(metrics);

        for (int i = 0; i < n; i++) {
            double[] curRow = trimmed.get(i);
            double[] origRow = metrics.get(i);
            for (int j = 0; j < curRow.length; j++) {
                if (curRow[j] < trimmer.bounds[j][1] && curRow[j] > trimmer.bounds[j][0]) {
                    assertEquals(curRow[j], origRow[j], 0);
                } else {
                    assertTrue(curRow[j] >= trimmer.bounds[j][0] && curRow[j] <= trimmer.bounds[j][1]);
                }
            }
        }
    }
}

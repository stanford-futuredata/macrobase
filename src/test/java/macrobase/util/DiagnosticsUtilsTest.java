package macrobase.util;

import macrobase.datamodel.Datum;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class DiagnosticsUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(DiagnosticsUtils.class);

    @Test
    /**
     * test 2D grid construction for a simple case.
     */
    public void testGrid() {
        double[][] boundaries = {
                {1, 2.001},
                {1, 2.001},
        };
        List<Datum> grid;

        grid = DiagnosticsUtils.create2DGrid(boundaries, 0.1);
        assertEquals(121, grid.size());

        grid = DiagnosticsUtils.create2DGrid(boundaries, 0.5);
        assertEquals(9, grid.size());

        double[][] expectedGrid = {
                {1, 1},
                {1, 1.5},
                {1, 2},
                {1.5, 1},
                {1.5, 1.5},
                {1.5, 2},
                {2, 1},
                {2, 1.5},
                {2, 2},
        };
        boolean[] pointsSeen = new boolean[9];
        for (Datum datum : grid) {
            double[] array = datum.getMetrics().toArray();
            for (int i = 0 ; i < 9 ; i++) {
                if (Math.abs(array[0] - expectedGrid[i][0]) < 0.01 &&  Math.abs(array[1] - expectedGrid[i][1]) < 0.01) {
                    pointsSeen[i] = true;
                    break;
                }
            }
        }
        for (int i = 0 ; i < 9 ; i++) {
            assertEquals(true, pointsSeen[i]);
        }
    }
}

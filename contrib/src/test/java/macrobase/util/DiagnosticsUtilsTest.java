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
    public void test2DGrid() {
        double[][] boundaries = {
                {1, 2.000001},
                {1, 2.000001},
        };
        List<Datum> grid;

        grid = DiagnosticsUtils.createGridFixedIncrement(boundaries, 0.1);
        assertEquals(121, grid.size());

        grid = DiagnosticsUtils.createGridFixedIncrement(boundaries, 0.5);
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
            double[] array = datum.metrics().toArray();
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

        List<Datum> grid2 = DiagnosticsUtils.createGridFixedSize(boundaries, 3);
        assertEquals(grid2.size(), 9);
        for (int i = 0; i < 9; i++) {
            for (int d=0; d < 2; d++) {
                assertEquals(grid.get(i).metrics().getEntry(d), grid2.get(i).metrics().getEntry(d), 1e-5);
            }
        }
    }

    @Test
    /**
     * test 1D grid construction for a simple case.
     */
    public void test1DGrid() {
        double[][] boundaries = {
                {1, 2.000001},
        };
        List<Datum> grid;
        grid = DiagnosticsUtils.createGridFixedIncrement(boundaries, 0.5);
        assertEquals(3, grid.size());
        grid = DiagnosticsUtils.createGridFixedSize(boundaries, 6);
        assertEquals(6, grid.size());
        assertEquals(1.8, grid.get(4).metrics().getEntry(0), 1e-5);
        assertEquals(1.2, grid.get(1).metrics().getEntry(0), 1e-5);

    }
}

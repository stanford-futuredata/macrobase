package macrobase.analysis.stats.kde;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AlgebraUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(AlgebraUtilsTest.class);

    @Test
    public void testBoundingBox() {
        double[][] matrixContents = {
                {1, 2, 3},
                {4, 5, 6},
                {7, 8, -9},
        };
        List<double[]> data = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            data.add(matrixContents[i]);
        }
        double[][] bbox = AlgebraUtils.getBoundingBoxRaw(data);

        assertEquals(1.0, bbox[0][0], 1e-10);
        assertEquals(7.0, bbox[0][1], 1e-10);
        assertEquals(-9.0, bbox[2][0], 1e-10);
        assertEquals(6.0, bbox[2][1], 1e-10);
    }
}

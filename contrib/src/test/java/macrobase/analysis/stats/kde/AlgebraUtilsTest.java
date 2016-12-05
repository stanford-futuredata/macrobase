package macrobase.analysis.stats.kde;

import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

        Assert.assertEquals(1.0, bbox[0][0], 1e-10);
        Assert.assertEquals(7.0, bbox[0][1], 1e-10);
        Assert.assertEquals(-9.0, bbox[2][0], 1e-10);
        Assert.assertEquals(6.0, bbox[2][1], 1e-10);
    }

    @Test
    public void testBoundingBoxDiff() {
        double[][] box1 = {
                {1,4},
                {2,5},
                {3,6}
        };
        double[][] box2 = {
                {0,1},
                {3,4},
                {9,10}
        };

        double[][] minMaxVectors = AlgebraUtils.getMinMaxDistanceBetweenBoxes(
                box1,box2
        );
        double[][] distanceVectors = {
                {0, 0, 3},
                {4, 2, 7}
        };
        for (int i = 0; i < minMaxVectors.length; i++) {
            for (int j = 0; j < minMaxVectors[0].length; j++) {
                Assert.assertEquals(distanceVectors[i][j], minMaxVectors[i][j], 1e-8);
            }
        }
    }
}

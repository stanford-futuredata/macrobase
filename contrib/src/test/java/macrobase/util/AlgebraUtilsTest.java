package macrobase.util;

import org.apache.commons.math3.linear.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static junit.framework.TestCase.assertEquals;

public class AlgebraUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(AlgebraUtilsTest.class);

    @Test
    /**
     * test that v^T M v == vec(M) vec(v v^T)
     */
    public void testVectorization() {
        double[][] matrixContents = {
                {1, 2, 3},
                {4, 5, 6},
                {7, 8, 9},
        };
        double[] flattenedMatrixContents = {
                1, 4, 7, 2, 5, 8, 3, 6, 9,
        };
        double[] vectorContents = {
                1, 2, 3,
        };

        RealMatrix matrix = new BlockRealMatrix(matrixContents);
        RealVector vectorizedMatrix = AlgebraUtils.vectorize(matrix);
        RealVector vector = new ArrayRealVector(vectorContents);
        assertEquals(vectorizedMatrix, new ArrayRealVector(flattenedMatrixContents));
        assertEquals(vector.dotProduct(matrix.operate(vector)), vectorizedMatrix.dotProduct(AlgebraUtils.vectorize(vector.outerProduct(vector))));
    }
}

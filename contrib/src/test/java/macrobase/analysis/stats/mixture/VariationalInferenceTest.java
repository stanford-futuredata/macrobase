package macrobase.analysis.stats.mixture;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertEquals;

public class VariationalInferenceTest {
    private static final Logger log = LoggerFactory.getLogger(VariationalInferenceTest.class);

    @Test
    public void sviStepTest() {
        assertEquals(0.9, VariationalInference.step(0, 1, 0.9), 1e-9);
        assertEquals(19.0, VariationalInference.step(10, 20, 0.9), 1e-9);
        assertEquals(12.0, VariationalInference.step(10, 20, 0.2), 1e-9);

        double[] array1 = {1, 2, 6};
        double[] array2 = {2, 3, 8};
        double[] array3 = {5, 6, 14};
        RealVector start = new ArrayRealVector(array1);
        RealVector end = new ArrayRealVector(array3);
        assertEquals(new ArrayRealVector(array2), VariationalInference.step(start, end, 0.25));

        double[][] matrix1 = {{1, 2}, {3, 10}};
        double[][] matrix2 = {{2, 6}, {3, 100}};
        double[][] matrix3 = {{5, 18}, {3, 370}};
        RealMatrix s = new BlockRealMatrix(matrix1);
        RealMatrix e = new BlockRealMatrix(matrix3);
        assertEquals(new BlockRealMatrix(matrix2), VariationalInference.step(s, e, 0.25));
    }
}

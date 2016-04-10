package macrobase.util;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

public class AlgebraUtils {
    public static RealMatrix createInverseMatrix(RealMatrix matrix) {
        if (matrix.getColumnDimension() > 1) {
            return MatrixUtils.blockInverse(matrix, (matrix.getColumnDimension() - 1) / 2);
        } else {
            // Manually invert size 1 x 1 matrix, because block Inverse requires dimensions > 1
            return MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(1. / matrix.getEntry(0, 0));
        }
    }
}

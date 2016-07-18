package macrobase.util;

import macrobase.datamodel.Datum;
import macrobase.datamodel.DatumComparator;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.util.Collections;
import java.util.List;

public class AlgebraUtils {
    public static RealMatrix invertMatrix(RealMatrix matrix) {
        if (matrix.getColumnDimension() > 1) {
            return MatrixUtils.blockInverse(matrix, (matrix.getColumnDimension() - 1) / 2);
        } else {
            // Manually invert size 1 x 1 matrix, because block Inverse requires dimensions > 1
            return MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(1. / matrix.getEntry(0, 0));
        }
    }

    public static RealVector vectorize(RealMatrix matrix) {
        int m = matrix.getColumnDimension();
        int n = matrix.getRowDimension();
        RealVector vector = matrix.getColumnVector(0);
        for (int i = 1; i < matrix.getRowDimension(); i++) {
            vector = vector.append(matrix.getColumnVector(i));
        }
        return vector;
    }

    /**
     * Returns coordinates of a bounding for surrounding data metrics.
     *
     * @param data
     * @return
     */
    public static double[][] getBoundingBox(List<Datum> data) {
        int D = data.get(0).getMetrics().getDimension();
        double[][] boundaries = new double[D][2];
        // Calculate boundaries of the data in the tree
        for (int i = 0; i < D; i++) {
            Datum maxI = Collections.max(data, new DatumComparator(i));
            Datum minI = Collections.min(data, new DatumComparator(i));
            boundaries[i][0] = minI.getMetrics().getEntry(i);
            boundaries[i][1] = maxI.getMetrics().getEntry(i);
        }
        return boundaries;
    }

    /**
     * Calculates determinant of a matrix using different algorithms based on dimensions.
     *
     * @param matrix
     * @return
     */
    public static double calcDeterminant(RealMatrix matrix) {
        assert matrix.getRowDimension() == matrix.getColumnDimension();
        if (matrix.getRowDimension() == 1) {
            return matrix.getEntry(0, 0);
        } else if (matrix.getRowDimension() == 2) {
            return matrix.getEntry(0, 0) * matrix.getEntry(1, 1) - matrix.getEntry(1, 0) * matrix.getEntry(0, 1);
        } else {
            return new LUDecomposition(matrix).getDeterminant();
        }
    }

    public static RealVector calcMean(List<RealVector> vectors) {
        RealVector mean = vectors.get(0);
        for (int i = 1; i < vectors.size(); i++) {
            mean = mean.add(vectors.get(i));
        }
        return mean.mapDivide(vectors.size());
    }
}

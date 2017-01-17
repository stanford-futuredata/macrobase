package macrobase.util;

import macrobase.datamodel.Datum;
import macrobase.datamodel.DatumComparator;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class AlgebraUtils {
    private static final Logger log = LoggerFactory.getLogger(AlgebraUtils.class);

    public static RealMatrix invertMatrix(RealMatrix matrix) {
        if (matrix.getColumnDimension() > 1) {
            return MatrixUtils.blockInverse(matrix, (matrix.getColumnDimension() - 1) / 2);
        } else {
            // Manually invert size 1 x 1 matrix, because block Inverse requires dimensions > 1
            return MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(1. / matrix.getEntry(0, 0));
        }
    }

    public static RealVector flattenMatrixByColumns(RealMatrix matrix) {
        int m = matrix.getColumnDimension(); // returns the width of the matrix, i.e. number of columns.
        int n = matrix.getRowDimension(); // returns the height of the matrix, i.e. number of rows.
        RealVector vector = matrix.getColumnVector(0);
        for (int i = 1; i < m; i++) {
            vector = vector.append(matrix.getColumnVector(i));
        }
        return vector;
    }

    public static RealMatrix reshapeMatrixByColumns(RealVector vector, RealMatrix shapeMatrix) {
        return AlgebraUtils.reshapeMatrixByColumns(vector, shapeMatrix.getColumnDimension(), shapeMatrix.getRowDimension());
    }

    public static RealMatrix reshapeMatrixByColumns(RealVector vector, int width, int height) {
        assert vector.getDimension() == width * height;
        RealMatrix matrix = new BlockRealMatrix(height, width);
        for (int i = 0; i < width; i++) {
            matrix.setColumnVector(i, vector.getSubVector(height * i, height));
        }
        return matrix;
    }

    /**
     * Returns coordinates of a bounding for surrounding data metrics.
     *
     * @param data
     * @return
     */
    public static double[][] getBoundingBox(List<Datum> data) {
        int D = data.get(0).metrics().getDimension();
        double[][] boundaries = new double[D][2];
        // Calculate boundaries of the data in the tree
        for (int i = 0; i < D; i++) {
            Datum maxI = Collections.max(data, new DatumComparator(i));
            Datum minI = Collections.min(data, new DatumComparator(i));
            boundaries[i][0] = minI.metrics().getEntry(i);
            boundaries[i][1] = maxI.metrics().getEntry(i);
        }
        return boundaries;
    }
}

package macrobase.util;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;

public class DiagnosticsUtils {

    public static List<Datum> create2DGrid(double[][] boundaries, double delta) {
        int dimension = boundaries.length;
        int[] dimensionPoints = new int[dimension];
        int size = 1;
        for (int d = 0; d < dimension; d++) {
            dimensionPoints[d] = (int) ((boundaries[d][1] - boundaries[d][0]) / delta + 1);
            size *= dimensionPoints[d];
        }
        List<Datum> scoredData = new ArrayList<>((int) size);
        double[] point = new double[dimension];
        for (int d = 0; d < dimension; d++) {
            point[d] = boundaries[d][0];
        }

        List<RealVector> gridPointVectors = new ArrayList<>(dimension);

        for (int d = 0; d < dimension; d++) {
            double[] array = new double[dimensionPoints[d]];
            for (int i = 0; i < dimensionPoints[d]; i++) {
                array[i] = boundaries[d][0] + i * delta;
            }
            gridPointVectors.add(new ArrayRealVector(array));
        }

        List<Datum> grid = new ArrayList<>(size);
        double[] array = new double[2];
        for (int i = 0; i < dimensionPoints[0]; i++) {
            array[0] = gridPointVectors.get(0).getEntry(i);
            for (int j = 0; j < dimensionPoints[1]; j++) {
                array[1] = gridPointVectors.get(1).getEntry(j);
                grid.add(new Datum(new ArrayList<Integer>(), new ArrayRealVector(array)));
            }
        }
        return grid;
    }
}

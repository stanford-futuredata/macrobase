package macrobase.analysis.stats.kde;

import org.apache.commons.math3.util.FastMath;

import java.util.Arrays;
import java.util.List;

public class AlgebraUtils {
    public static double[][] getBoundingBoxRaw(List<double[]> data) {
        int d = data.get(0).length;
        double[][] boundaries = new double[d][2];
        // Calculate boundaries of the data in the tree
        for (int i = 0; i < d; i++) {
            double maxI = -Double.MAX_VALUE;
            double minI = Double.MAX_VALUE;
            for (int j = 0; j < data.size(); j++) {
                double cur = data.get(j)[i];
                if (cur < minI) {minI = cur;}
                if (cur > maxI) {maxI = cur;}
            }
            boundaries[i][0] = minI;
            boundaries[i][1] = maxI;
        }
        return boundaries;
    }

    /**
     * @param box1 box[d][0,1] are boundaries along dimension d
     * @param box2
     * @return two row vectors
     */
    public static double[][] getMinMaxDistanceBetweenBoxes(double[][] box1, double[][] box2) {
        int k = box1.length;
        double[][] minMaxDiff = new double[2][k];

        for (int i=0; i<k; i++) {
            double d1 = box2[i][0] - box1[i][1];
            double d2 = box1[i][0] - box2[i][1];
            // box2 to left
            if (d2 >= 0) {
                minMaxDiff[0][i] = d2;
                minMaxDiff[1][i] = -d1;
            }
            // box2 to right
            else if (d1 >= 0) {
                minMaxDiff[0][i] = d1;
                minMaxDiff[1][i] = -d2;
            }
            // inside
            else {
                d1 = FastMath.abs(d1);
                d2 = FastMath.abs(d2);
                minMaxDiff[0][i] = 0;
                minMaxDiff[1][i] = d1 > d2 ? d1 : d2;
            }
        }

        return minMaxDiff;
    }

    public static String array2dToString(double[][] data) {
        StringBuilder s = new StringBuilder();
        s.append("[");
        for (double[] curRow : data) {
            s.append(Arrays.toString(curRow));
            s.append(",");
        }
        s.append("]");
        return s.toString().trim();
    }
}

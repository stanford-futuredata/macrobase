package msolver.util;

public class MatrixUtil {
    public static void fMatVec(
            int n,
            int m,
            double[] A,
            double[] x,
            double[] out
    ) {
        for (int i = 0; i < n; i++) {
            double sum = 0;
            for (int j = 0; j < m; j++) {
                sum += A[i*m+j]*x[j];
            }
            out[i] = sum;
        }
    }

    public static void fMatVecLeft(
            int n,
            int m,
            double[] A,
            double[] x,
            double[] out
    ) {
        for (int j = 0; j < m; j++) {
            double sum = 0;
            for (int i = 0; i < n; i++) {
                sum += x[i]*A[i*m+j];
            }
            out[j] = sum;
        }
    }

    public static void matVec(
            double[][] A,
            double[] x,
            double[] out
    ) {
        int n = A.length;
        int m = A[0].length;
        for (int i = 0; i < n; i++) {
            double sum = 0;
            for (int j = 0; j < m; j++) {
                sum += A[i][j]*x[j];
            }
            out[i] = sum;
        }
    }

    public static void matVecLeft(
            double[][] A,
            double[] x,
            double[] out
    ) {
        int n = A.length;
        int m = A[0].length;
        for (int j = 0; j < m; j++) {
            double sum = 0;
            for (int i = 0; i < n; i++) {
                sum += x[i]*A[i][j];
            }
            out[j] = sum;
        }
    }
}

package msolver.chebyshev;

import org.apache.commons.math3.util.FastMath;

public class QuadraticCosFunction implements CosScaledFunction {
    private int k;
    public QuadraticCosFunction(int k) {
        this.k = k;
    }

    @Override
    public int numFuncs() {
        return k;
    }

    @Override
    public double[][] calc(int N) {
        double[] cosValues = new double[N+1];
        for (int j = 0; j <= N; j++) {
            cosValues[j] = FastMath.cos(j * Math.PI / N);
        }

        double[][] values = new double[k][N+1];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j <= N; j++) {
                values[i][j] = (i+1)*cosValues[j]*cosValues[j];
            }
        }
        return values;
    }
}

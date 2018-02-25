package msolver.optimizer;

/**
 * Simple quadratic function for use in tests.
 */
public class QuadraticPotential implements FunctionWithHessian {
    private int k;
    private double Pval;
    private double[] Pgrad;
    private double[][] Phess;

    public QuadraticPotential(int k) {
        this.k = k;
        Pgrad = new double[k];
        Phess = new double[k][k];
    }

    @Override
    public void computeOnlyValue(double[] point, double tol) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += point[i] * point[i];
        }
        Pval = sum;
    }

    @Override
    public void computeAll(double[] point, double tol) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += point[i] * point[i];
        }
        Pval = sum;

        for (int i = 0; i < point.length; i++) {
            Pgrad[i] = 2*point[i];
            for (int j = 0; j < point.length; j++) {
                if (j == i) {
                    Phess[i][j] = 2;
                } else {
                    Phess[i][j] = 0.0;
                }
            }
        }
    }

    @Override
    public int dim() {
        return this.k;
    }

    @Override
    public double getValue() {
        return Pval;
    }

    @Override
    public double[] getGradient() {
        return Pgrad;
    }

    @Override
    public double[][] getHessian() {
        return Phess;
    }
}

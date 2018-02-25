package msolver.optimizer;

/**
 * Describes a function which can be optimized using Newton's method.
 */
public interface FunctionWithHessian {
    void computeOnlyValue(double[] point, double tol);
    void computeAll(double[] point, double tol);
    int dim();
    double getValue();
    double[] getGradient();
    // Returns in row-major order
    double[][] getHessian();
}

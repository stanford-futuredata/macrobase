package msolver;

import msolver.optimizer.FunctionWithHessian;

/**
 * Minimizing this function yields a maxent pdf which matches the the empirical
 * moments of a dataset. The function is convex with symmetric positive definite
 * hessian and has a global stationary minimum (gradient = 0) at the solution.
 */
public class MaxEntPotential implements FunctionWithHessian {
    protected double[] d_mus;

    private int cumFuncEvals = 0;
    protected double[] lambd;
    protected double[] mus;
    protected double[] grad;
    protected double[][] hess;

    public MaxEntPotential(
            double[] d_mus
    ) {
        this.d_mus = d_mus;

        this.cumFuncEvals = 0;

        int k = d_mus.length;
        this.mus = new double[k];
        this.grad = new double[k];
        this.hess = new double[k][k];
    }

    @Override
    public int dim() {
        return d_mus.length;
    }

    @Override
    public void computeOnlyValue(double[] point, double tol) {
        computeAll(point, tol);
    }

    @Override
    public void computeAll(double[] lambd, double tol) {
        this.lambd = lambd;
        int k = lambd.length;
        MaxEntFunction f = new MaxEntFunction(lambd);
        this.mus = f.moments(k*2, tol);
        this.cumFuncEvals += f.getFuncEvals();

        for (int i = 0; i < k; i++) {
            this.grad[i] = d_mus[i] - mus[i];
        }
        for (int i=0; i < k; i++) {
            for (int j=0; j <= i; j++) {
                this.hess[i][j] = (mus[i+j] + mus[i-j])/2;
            }
        }
        for (int i=0; i < k; i++) {
            for (int j=i+1; j < k; j++) {
                this.hess[i][j] = hess[j][i];
            }
        }
    }

    @Override
    public double getValue() {
        double sum = 0.0;
        int k = d_mus.length;
        for (int i = 0; i < k; i++) {
            sum += lambd[i] * d_mus[i];
        }
        return this.mus[0] + sum;
    }

    @Override
    public double[] getGradient() {
        return grad;
    }

    @Override
    public double[][] getHessian() {
        return hess;
    }

    public int getCumFuncEvals() {
        return cumFuncEvals;
    }
}

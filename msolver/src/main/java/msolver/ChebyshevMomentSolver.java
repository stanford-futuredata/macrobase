package msolver;

import msolver.chebyshev.ChebyshevPolynomial;
import msolver.optimizer.NewtonOptimizer;
import org.apache.commons.math3.analysis.solvers.BrentSolver;
import org.apache.commons.math3.analysis.solvers.UnivariateSolver;

import java.util.Arrays;

public class ChebyshevMomentSolver {
    private double[] d_mus;
    private boolean verbose = false;

    private double[] lambdas;
    private ChebyshevPolynomial approxCDF;
    private boolean isConverged;

    private NewtonOptimizer optimizer;
    private int cumFuncEvals;

    public ChebyshevMomentSolver(double[] chebyshev_moments) {
        d_mus = chebyshev_moments;
    }

    public static ChebyshevMomentSolver fromPowerSums(
            double min, double max, double[] powerSums
    ) {
        double[] scaledChebyMoments = MathUtil.powerSumsToChebyMoments(
                min, max, powerSums
        );
        return new ChebyshevMomentSolver(scaledChebyMoments);
    }

    public void setVerbose(boolean flag) {
        this.verbose = flag;
    }

    public int solve(double tol) {
        double[] l_initial = new double[d_mus.length];
        return solve(l_initial, tol);
    }

    public int solve(double[] l_initial, double tol) {
        MaxEntPotential potential = new MaxEntPotential(d_mus);
        optimizer = new NewtonOptimizer(potential);
        optimizer.setVerbose(verbose);
        lambdas = optimizer.solve(l_initial, tol);
        isConverged = optimizer.isConverged();
        if (verbose) {
            System.out.println("Final Polynomial: " + Arrays.toString(lambdas));
        }
        cumFuncEvals = potential.getCumFuncEvals();

        approxCDF = ChebyshevPolynomial.fit(new MaxEntFunction(lambdas), tol).integralPoly();
        return optimizer.getStepCount();
    }

    public double[] estimateQuantiles(double[] ps, double min, double max) {
        UnivariateSolver bSolver = new BrentSolver(1e-6);
        int n = ps.length;
        double c = (max + min) / 2;
        double r = (max - min) / 2;
        double[] quantiles = new double[n];

        for (int i = 0; i < n; i++) {
            double p = ps[i];
            double q;
            if (p <= 0.0) {
                q = -1;
            } else if (p >= 1.0) {
                q = 1;
            } else {
                q = bSolver.solve(
                        100,
                        (x) -> approxCDF.value(x) - p,
                        -1,
                        1,
                        0
                );
            }
            quantiles[i] = q*r+c;
        }
        return quantiles;
    }

    public double estimateCDF(double x) {
        return approxCDF.value(x);
    }

    public double[] getLambdas() {
        return lambdas;
    }

    public NewtonOptimizer getOptimizer() {
        return optimizer;
    }
    public int getCumFuncEvals() {
        return cumFuncEvals;
    }
    public double[] getChebyshevMoments() { return d_mus; }

    public boolean isConverged() {
        return isConverged;
    }
}

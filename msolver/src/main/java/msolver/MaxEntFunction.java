package msolver;

import msolver.chebyshev.ChebyshevPolynomial;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.util.FastMath;

/**
 * Solutions to the maximum entropy moment problem have the form exp(-poly(x)).
 * It is useful to express the polynomial in a chebyshev basis for better conditioning.
 */
public class MaxEntFunction implements UnivariateFunction{
    private ChebyshevPolynomial p;
    private ChebyshevPolynomial p_approx;
    private int funcEvals;

    public int getFuncEvals() {
        return funcEvals;
    }

    public MaxEntFunction(double[] coeffs) {
        this.p = new ChebyshevPolynomial(coeffs);
        this.funcEvals = 0;
    }

    @Override
    public double value(double v) {
        return FastMath.exp(-p.value(v));
    }

    public double[] moments(int mu_k, double tol) {
        p_approx = ChebyshevPolynomial.fit(this, tol);
        funcEvals += p_approx.getNumFitEvals();
        double[] out_moments = new double[mu_k];
        for (int i = 0; i < mu_k; i++) {
            ChebyshevPolynomial p_times_moment = p_approx.multiplyByBasis(i);
            out_moments[i] = p_times_moment.integrate();
        }
        return out_moments;
    }
}

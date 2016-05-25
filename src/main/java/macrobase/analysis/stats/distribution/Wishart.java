package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.special.Gamma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Wishart {
    private static final Logger log = LoggerFactory.getLogger(Wishart.class);
    private final double logDetOmega;
    private RealMatrix omega;
    private double nu;
    private int D;  // dimensions

    public Wishart(RealMatrix omega, double nu) {
        this.omega = omega;
        this.nu = nu;
        this.D = omega.getColumnDimension();
        if (omega.getRowDimension() == 2) {
            this.logDetOmega = Math.log(omega.getEntry(0, 0) * omega.getEntry(1, 1) - omega.getEntry(1, 0) * omega.getEntry(0, 1));
        } else {
            this.logDetOmega = Math.log((new EigenDecomposition(omega)).getDeterminant());
        }
    }

    /**
     * B is the normalizing factor of the Wishart Distribution,
     * @return log value of the normalizing factor.
     */
    public double lnB() {
        double lnB = -0.5 * nu * logDetOmega
                - 0.5 * nu * D * Math.log(2)
                - 0.25 * D * (D - 1) * Math.log(Math.PI);
        for (int i = 1; i <= D; i++) {
            lnB -= Gamma.logGamma(0.5 * (nu + 1 - i));
        }
        return lnB;
    }

    private double expectationLnLambda() {
        double ex_ln_lambda = D * Math.log(2) + logDetOmega;
        for (int i = 1; i <= D; i++) {
            ex_ln_lambda += Gamma.digamma(0.5 * (nu + 1 - i));
        }
        return ex_ln_lambda;
    }

    public double getEntropy() {
        return -lnB() - 0.5 * (nu - D - 1) * expectationLnLambda() + nu * D / 2.;
    }

    public double getExpectationLogDeterminantLambda() {
        double ex_log_lambda = D * Math.log(2) + logDetOmega;
        for (int i=0; i<D; i++) {
            ex_log_lambda += Gamma.digamma(0.5 * (nu - i));
        }
        return ex_log_lambda;
    }
}

package msolver.chebyshev;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.transform.DctNormalization;
import org.apache.commons.math3.transform.FastCosineTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.commons.math3.util.FastMath;

import java.util.Arrays;

/**
 * Utility class to fit polynomials to arbitrary functions and then
 * compute integrals (effectively clenshaw-curtis integration). See
 * the chebfun library for inspiration.
 *
 * Notably: by fitting a chebyshev polynomial to a function once can then
 * compute all of its chebyshev moments without re-evaluating the function.
 */
public class ChebyshevPolynomial implements UnivariateFunction {
    private double[] coeffs;
    private int numFitEvals;

    public ChebyshevPolynomial(double[] coeff) {
        this.coeffs = coeff;
    }

    public double[] coeffs() {
        return coeffs;
    }

    public int size() {
        return coeffs.length;
    }

    public static ChebyshevPolynomial basis(int k) {
        double[] basisCoeffs = new double[k+1];
        basisCoeffs[k] = 1.0;
        return new ChebyshevPolynomial(basisCoeffs);
    }

    public static ChebyshevPolynomial[] fitMulti(
            CosScaledFunction fMulti,
            double tol
    ) {
        int N = 64;
        int nFuncs = fMulti.numFuncs();
        double[][] cs = new double[nFuncs][];
        double oldError = Double.MAX_VALUE;
        FastCosineTransformer t = new FastCosineTransformer(
                DctNormalization.STANDARD_DCT_I
        );
        double error;
        while (true) {
            double[][] fVals = fMulti.calc(N);

            error = 0.0;
            for (int i = 0; i < nFuncs; i++) {
                cs[i] = t.transform(fVals[i], TransformType.FORWARD);
                for (int j = 0; j <= N; j++) {
                    cs[i][j] *= 2.0/N;
                }
                cs[i][0] /= 2;

                // calculate the maximum error over all of the approximations
                for (int j = 1; j <= 5; j+= 2) {
                    error = Math.max(Math.abs(cs[i][cs[i].length - j]), error);
                }
            }

            if (error < tol || N > 4000) {
                break;
            } else {
                N *= 2;
                oldError = error;
            }
        }
//        System.out.println("cheby n: "+N+" error: "+error);

        ChebyshevPolynomial[] results = new ChebyshevPolynomial[nFuncs];
        for (int i = 0; i < nFuncs; i++) {
            results[i] = new ChebyshevPolynomial(cs[i]);
        }
        return results;
    }

    public static ChebyshevPolynomial fit(
            UnivariateFunction f,
            double tol
    ) {
        int N = 32;
        double[] cs;
        int numEvals = 0;
        double[] oldFVals = null;
        double oldError = Double.MAX_VALUE;
        while(true) {
            double[] fvals = new double[N+1];
            if (oldFVals == null) {
                for (int i = 0; i <= N; i++) {
                    fvals[i] = f.value(FastMath.cos(Math.PI * i / N));
                }
                numEvals += (N+1);
            } else {
                for (int i = 0; i <= N; i++) {
                    if (i % 2 == 1) {
                        fvals[i] = f.value(FastMath.cos(Math.PI * i / N));
                    } else {
                        fvals[i] = oldFVals[i/2];
                    }
                }
                numEvals += N/2;
            }
            oldFVals = fvals;
            FastCosineTransformer t = new FastCosineTransformer(
                    DctNormalization.STANDARD_DCT_I
            );
            cs = t.transform(fvals, TransformType.FORWARD);
            for (int i = 0; i <= N; i++) {
                cs[i] *= 2.0/N;
            }

            double error = 0.0;
            double e1 = FastMath.abs(cs[cs.length-1]);
            if (e1 > error) {error = e1;}
            double e2 = 2*FastMath.abs(cs[cs.length-3]);
            if (e2 > error) {error = e2;}
            double e3 = 2*FastMath.abs(cs[cs.length-5]);
            if (e3 > error) {error = e3;}
//            System.out.println("cheby n: "+N+" error: "+error);

            if (error < tol || N > 1000) {
                break;
            } else {
                N *= 2;
                oldError = error;
            }
        }
        cs[0] /= 2;
        ChebyshevPolynomial result = new ChebyshevPolynomial(cs);
        result.numFitEvals = numEvals;
        return result;
    }

    public ChebyshevPolynomial multiplyByBasis(int k) {
        if (k == 0) {
            return new ChebyshevPolynomial(coeffs.clone());
        }
        double[] newCoeffs = new double[coeffs.length+k];
        for (int i = 0; i < coeffs.length; i++) {
            double c2 = coeffs[i] / 2;
            newCoeffs[i + k] += c2;
            if ( i < k) {
                newCoeffs[k - i] += c2;
            } else {
                newCoeffs[i - k] += c2;
            }
        }
        return new ChebyshevPolynomial(newCoeffs);
    }

    /**
     * This function appears to be unreliable for large polynomials
     * See https://arxiv.org/pdf/1009.4597.pdf
     * @param p2 factor
     * @return new product
     */
    public ChebyshevPolynomial multiply(ChebyshevPolynomial p2) {
        double[] c = new double[coeffs.length + p2.coeffs.length - 1];
        int d = Math.max(coeffs.length, p2.coeffs.length)-1;
        double[] a = Arrays.copyOf(coeffs, d+1);
        double[] b = Arrays.copyOf(p2.coeffs, d+1);
        a[0] *= 2;
        b[0] *= 2;
        double sum;
        // k = 0
        sum = a[0] * b[0];
        for (int l = 1; l <= d; l++) {
            sum += a[l]*b[l]*2;
        }
        c[0] = sum / 2;

        // 1 <= k <= d1
        for (int k = 1; k <= d-1; k++) {
            sum = 0;
            for (int l = 0; l <= k; l++) {
                sum += a[k-l]*b[l];
            }
            for (int l = 1; l <= d-k; l++) {
                sum += a[l]*b[k+l] + a[k+l]*b[l];
            }
            c[k] = sum/2;
        }

        for (int k = d; k < c.length; k++) {
            sum = 0;
            for (int l = k-d; l <= d; l++) {
                sum += a[k-l]*b[l];
            }
            c[k] = sum/2;
        }

        c[0] /= 2;
        return new ChebyshevPolynomial(c);
    }

    public double integrate() {
        double sum = 0.0;
        for (int i2 = 0; i2 < coeffs.length; i2+=2) {
            sum -= coeffs[i2]/((i2+1)*(i2-1));
        }
        return 2*sum;
    }

    public ChebyshevPolynomial integralPoly() {
        int k = coeffs.length;
        double[] integCoeffs = new double[k+1];
        integCoeffs[1] = coeffs[0];
        integCoeffs[2] = coeffs[1] / 4;
        for (int i = 2; i < k; i++) {
            integCoeffs[i+1] += coeffs[i] / (2*(i+1));
            integCoeffs[i-1] -= coeffs[i] / (2*(i-1));
        }
        // normalize so that integral(-1) = 0
        for (int i = 1; i < k+1; i++) {
            if (i % 2 == 0) {
                integCoeffs[0] -= integCoeffs[i];
            } else {
                integCoeffs[0] += integCoeffs[i];
            }
        }
        return new ChebyshevPolynomial(integCoeffs);
    }

    public double value(double x) {
        int k = coeffs.length;
        double sum = 0.0;
        double ts0 = 1.0;
        double ts1 = x;

        sum += coeffs[0];
        if (k > 1) {
            sum += coeffs[1] * x;
            for (int i = 2; i < k; i++) {
                double tt1 = ts1;
                ts1 = 2*x*ts1 - ts0;
                ts0 = tt1;
                sum += coeffs[i] * ts1;
            }
        }

        return sum;
    }

    public double value2(double x) {
        double bk0=0,bk1=0, bk2=0;
        for (int i = coeffs.length-1; i > 0; i--) {
            bk2=bk1;
            bk1=bk0;
            bk0=coeffs[i] + 2*x*bk1 - bk2;
        }
        return coeffs[0]+x*bk0-bk1;
    }

    @Override
    public String toString() {
        return "CPoly: "+ Arrays.toString(coeffs);
    }

    public int getNumFitEvals() {
        return numFitEvals;
    }
}

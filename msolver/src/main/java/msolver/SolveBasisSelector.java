package msolver;

import msolver.chebyshev.ChebyshevPolynomial;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import java.util.ArrayList;
import java.util.List;

/**
 * Choose how many powers / log powers to use when solving for
 * maximum entropy.
 */
public class SolveBasisSelector {
    private double maxConditionNumber = 1000;
    private double tol = 1e-5;

    private int ka, kb;

    public SolveBasisSelector() {

    }

    public double[][] getHessian(
            int ka,
            List<UnivariateFunction> gs
    ) {
        int kb = gs.size();
        int n = ka+kb;
        double[][] hess = new double[n][n];
        for (int i = 0; i < ka; i++)  {
            for (int j = 0; j < ka; j++) {
                if ( (i + j) % 2 == 0) {
                    double s = i+j;
                    double d = i-j;
                    hess[i][j] = -(1/(s*s - 1) + 1/(d*d - 1));
                }
            }
        }
        List<ChebyshevPolynomial> cps = new ArrayList<>();
        for (UnivariateFunction g : gs) {
            ChebyshevPolynomial curFitted = ChebyshevPolynomial.fit(g, tol);
            cps.add(curFitted);
        }
        for (int j=0; j<kb; j++) {
            for (int i=j; i<kb; i++) {
                hess[i+ka][j+ka] = cps.get(i).multiply(cps.get(j)).integrate();
            }
        }
        for (int j=0; j<ka; j++) {
            for (int i=0; i<kb; i++) {
                hess[i+ka][j] = cps.get(i).multiplyByBasis(j).integrate();
            }
        }
        for (int j=0; j < n; j++) {
            for (int i = 0; i < j; i++) {
                hess[i][j] = hess[j][i];
            }
        }
        return hess;
    }

    public void select(
            boolean useStandardBasis, double[] aMoments, double[] bMoments,
            double aCenter, double aScale, double bCenter, double bScale
    ) {
        // only use moments that are < 1, others are the product of numeric issues
        int maxKa = aMoments.length;
        for (int i = 0; i < aMoments.length; i++) {
            if (Math.abs(aMoments[i]) > 1.1) {
                maxKa = i;
                break;
            }
        }
        int maxKb = bMoments.length;
        for (int i = 0; i < bMoments.length; i++) {
            if (Math.abs(bMoments[i]) > 1.1) {
                maxKb = i;
                break;
            }
        }

        ka = maxKa;
        for (int nBMoments = 0; nBMoments < maxKb; nBMoments++) {
            kb = nBMoments+1;
            List<UnivariateFunction> gFunctions = new ArrayList<>();
            for (int i = 1; i < kb; i++) {
                gFunctions.add(
                        new GFunction(
                                i, useStandardBasis,
                                aCenter, aScale, bCenter, bScale
                        )
                );
            }
            double[][] hess = getHessian(ka, gFunctions);
//
//            MaxEntFunction2 f = new MaxEntFunction2(
//                    useStandardBasis,
//                    new double[ka],
//                    new double[kb],
//                    aCenter,aScale,
//                    bCenter,bScale
//            );
//            double[][] hess2 = f.getHessian(tol);
//            RealMatrix m2 = new Array2DRowRealMatrix(hess2, false);
//            double c2 = new SingularValueDecomposition(m2).getConditionNumber();

            RealMatrix m = new Array2DRowRealMatrix(hess, false);
            double c = new SingularValueDecomposition(m).getConditionNumber();
//            System.out.println("ka: "+ka+" kb: "+kb+" c: "+c);
            if (c > maxConditionNumber || !Double.isFinite(c)) {
                kb = Math.max(1, kb-1);
                break;
            }
        }
    }

    public void setMaxConditionNumber(double maxConditionNumber) {
        this.maxConditionNumber = maxConditionNumber;
    }

    public int getKa() {
        return ka;
    }
    public int getKb() {
        return kb;
    }
}

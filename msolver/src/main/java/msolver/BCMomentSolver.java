package msolver;

import msolver.chebyshev.ChebyshevPolynomial;
import msolver.util.MathUtil;
import msolver.util.MatrixUtil;
import org.apache.commons.math3.analysis.solvers.BrentSolver;
import org.apache.commons.math3.analysis.solvers.UnivariateSolver;
import org.apache.commons.math3.transform.DctNormalization;
import org.apache.commons.math3.transform.FastCosineTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.commons.math3.util.FastMath;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.CommonOps_DDRM;
import org.ejml.dense.row.NormOps_DDRM;
import org.ejml.dense.row.factory.LinearSolverFactory_DDRM;
import org.ejml.interfaces.linsol.LinearSolverDense;

import java.util.Arrays;

public class BCMomentSolver {
    private boolean verbose = false;
    private int nBins = 64;
    private int nDeg;

    private double[] d_mus;

    private final double[] x;
    private double[] f;
    private double[] G;
    private double[] cs;
    private double[] grad;
    private double[] e_mu;
    private double[] Hess;
    private double[] lambdas;
    private double[] newLambdas;
    private DMatrixRMaj stepVec;

    private int pdfResolution = 1024;
    private double[] xValues;
    private double[] pdfValues;
    private double[] cdfValues;
    private ChebyshevPolynomial polyPDF, polyCDF;

    private int maxSteps = 15;
    private double tolerance = 1e-6;
    private boolean isConverged;

    public BCMomentSolver(
            int nBins,
            int nDeg
    ) {
        this.nBins = nBins;
        this.nDeg = nDeg;

        int n = nBins+1;
        int k = nDeg;
        x = new double[n];
        G = new double[2*k*n];

        lambdas = new double[k];
        newLambdas = new double[k];
        f = new double[n];
        e_mu = new double[2*k];
        grad = new double[k];
        Hess = new double[k*k];
        stepVec = new DMatrixRMaj(k,1);

        for (int i=0; i<n; i++) {
            x[i] = Math.cos(Math.PI * i / nBins);
//            x[i] = Math.cos(Math.PI * (i+.5) / nBins);
        }
        for (int i=0; i<2*k; i++) {
            for (int j=0; j<n; j++) {
                if (i == 0) {
                    G[i*n+j] = 1.0;
                } else if (i == 1){
                    G[i*n+j] = x[j];
                } else {
                    G[i*n+j] = 2*x[j]* G[(i-1)*n+j] - G[(i-2)*n+j];
                }
            }
        }
    }

    public void setVerbose(boolean flag) {
        this.verbose = flag;
    }
    public void setMaxSteps(int maxSteps) {
        this.maxSteps = maxSteps;
    }
    public void setTolerance(double tol) {
        this.tolerance = tol;
    }

    private void calcGrad(double[] l) {
        int n = nBins+1;
        int k = d_mus.length;
        MatrixUtil.fMatVecLeft(k, n, G, l, f);
        for (int i = 0; i < n; i++) {
            f[i] = FastMath.exp(f[i]);
        }
        FastCosineTransformer t = new FastCosineTransformer(
                DctNormalization.STANDARD_DCT_I
        );
        cs = t.transform(f, TransformType.FORWARD);
//        DoubleDCT_1D dct = new DoubleDCT_1D(n);
//        cs = f.clone();
//        dct.forward(cs, false);

        for (int i = 0; i < n; i++) {
            cs[i] *= 2.0/nBins;
        }
        cs[0] /= 2;
//        System.out.println("cs: "+Arrays.toString(cs));
        // optimize
        for (int i = 0; i < e_mu.length; i++) {
            double sum = 0.0;
            for (int j = 0; j < cs.length; j++) {
                if ((i+j) % 2 == 0) {
                    int j1 = i + j;
                    int j2 = Math.abs(i - j);
                    sum -= cs[j] * (1.0 / (j1 * j1 - 1) + 1.0 / (j2 * j2 - 1));
                }
            }
            e_mu[i] = sum;
        }

        for (int i = 0; i < k; i++) {
            grad[i] = e_mu[i] - d_mus[i];
        }
    }

    private void calcHessian() {
        int k = d_mus.length;

        for (int i = 0; i < k; i++) {
            for (int j = 0; j <= i; j++) {
                Hess[i*k + j] = (e_mu[i+j] + e_mu[i - j])/2;
            }
        }
        for (int i = 0; i < k; i++) {
            for (int j = i+1; j < k; j++) {
                Hess[i*k+j] = Hess[j*k+i];
            }
        }
    }

    public int solve(
            double[] chebyMoments
    ) {
        this.isConverged = false;
        this.d_mus = chebyMoments;

        int k = d_mus.length;
        DMatrixRMaj hhMat = DMatrixRMaj.wrap(k, k, Hess);
        DMatrixRMaj gradVec = DMatrixRMaj.wrap(k, 1, grad);
        LinearSolverDense<DMatrixRMaj> solver = LinearSolverFactory_DDRM.chol(k);

        int stepCount;
        for(stepCount = 0; stepCount < maxSteps; stepCount++) {
            calcGrad(lambdas);
            calcHessian();

            solver.setA(hhMat);
            solver.solve(gradVec, stepVec);
            CommonOps_DDRM.scale(-1.0, stepVec);

            double gradNorm = NormOps_DDRM.fastNormP2(gradVec);
            double pVal = e_mu[0] - MathUtil.dot(lambdas, d_mus);
            double dfdx = CommonOps_DDRM.dot(gradVec, stepVec);
            if (verbose) {
                System.out.println(String.format("Step: %d, Grad: %g, Pval: %g",
                        stepCount,
                        gradNorm,
                        pVal
                        ));
            }
            if (gradNorm < tolerance) {
                isConverged = true;
                break;
            }

            double stepScaleFactor = 1.0;
            double alpha = .3;
            double beta = .25;

            while (true) {
                for (int i = 0; i < k; i++) {
                    newLambdas[i] = lambdas[i] + stepScaleFactor*stepVec.get(i);
                }
                calcGrad(newLambdas);
                double pValNew = e_mu[0] - MathUtil.dot(newLambdas, d_mus);
                double gradNormNew = NormOps_DDRM.fastNormP2(gradVec);

                double deltaChange = pVal + alpha*stepScaleFactor*dfdx - pValNew;

                if (deltaChange > -1e-6 || stepScaleFactor < 1e-5) {
                    break;
                } else {
                    stepScaleFactor *= beta;
                }
                if (verbose) {
                    System.out.println(String.format(
                            "LineSearch: %g, delta: %g",
                            stepScaleFactor,
                            deltaChange
                    ));
                }
            }
            for (int i = 0; i < k; i++) {
                lambdas[i] = newLambdas[i];
            }
        }

        if(verbose) {
            System.out.println("Final Lambdas: ");
            System.out.println(Arrays.toString(lambdas));
        }
        ChebyshevPolynomial expPoly = new ChebyshevPolynomial(lambdas);

        polyPDF = ChebyshevPolynomial.fit(
                (double x) -> {
                    return FastMath.exp(expPoly.value(x));
                },
                tolerance
        );
        polyCDF = polyPDF.integralPoly();
        if (verbose) {
            System.out.println(String.format("CDF Max: %g", polyCDF.value(1)));
        }
        return stepCount;
    }

    public ChebyshevPolynomial getPolyCDF() {
        return polyCDF;
    }

    public double getQuantile(double p) {
        double pMax = polyCDF.value(1);
        double pAdj = p * pMax;
        if (pAdj <= 0) {
            return -1;
        } else if (pAdj >= pMax) {
            return 1;
        }
        UnivariateSolver bSolver = new BrentSolver(tolerance);
        double q = bSolver.solve(
                100,
                (x) -> polyCDF.value(x) - pAdj,
                -1,
                1,
                0
        );
        return q;
    }
}

package msolver;

import msolver.util.MatrixUtil;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.CommonOps_DDRM;
import org.ejml.dense.row.factory.LinearSolverFactory_DDRM;
import org.ejml.interfaces.linsol.LinearSolverDense;

public class BinnedMomentSolver {
    private boolean verbose = false;
    private int nBins = 200;
    private int nDeg;

    private double[] d_mus;
    private double aCenter, aScale;

    private double[] x;
    private double[] f;
    private double[] Af;
    private double[] grad;
    private double[] e_mu;
    private double[] Hess;
    private double[] lambdas;
    private DMatrixRMaj stepVec;

    private int maxSteps = 20;
    private double tolerance = 0.01;
    private boolean isConverged;

    public BinnedMomentSolver(
            int nBins,
            int nDeg
    ) {
        this.nBins = nBins;
        this.nDeg = nDeg;

        int n = nBins+1;
        int k = nDeg;
        x = new double[n];
        f = new double[n];
        Af = new double[2*k*n];
        e_mu = new double[2*k];

        lambdas = new double[k];
        f = new double[n];
        grad = new double[k];
        Hess = new double[k*k];
        stepVec = new DMatrixRMaj(k,1);
    }

    private void solveSetup() {
        int n = nBins+1;
        int k = d_mus.length;
        for (int i=0; i<n; i++) {
            x[i] = (i*1.0/nBins)*2-1;
        }
        for (int i=0; i<2*k; i++) {
            for (int j=0; j<n; j++) {
                if (i == 0) {
                    Af[i*n+j] = 1.0;
                } else if (i == 1){
                    Af[i*n+j] = x[j];
                } else {
                    Af[i*n+j] = 2*x[j]*Af[(i-1)*n+j] - Af[(i-2)*n+j];
                }
            }
        }
    }

    private void calcGrad() {
        int n = nBins+1;
        int k = d_mus.length;
        MatrixUtil.fMatVecLeft(k, n, Af, lambdas, f);
        for (int i = 0; i < n; i++) {
            // Exponential approximation
            final long tmp = (long) (1512775 * f[i] + 1072632447);
            f[i] = Double.longBitsToDouble(tmp << 32);
        }
        MatrixUtil.fMatVec(2*k, n, Af, f, e_mu);
        for (int i = 0; i < k; i++) {
            grad[i] = e_mu[i] - d_mus[i];
        }
    }

    private void calcHessian() {
        int k = d_mus.length;

        for (int i = 0; i < k; i++) {
            for (int j = 0; j <= i; j++) {
                Hess[i*k + j] = (e_mu[i+j] + e_mu[i - j])/2;
//                    double sum = 0;
//                    for (int z = 0; z < n; z++) {
//                        sum += f[z]*A[i][z]*A[j][z];
//                    }
//                    Hess[i*k+j] = sum;
            }
        }
        for (int i = 0; i < k; i++) {
            for (int j = i+1; j < k; j++) {
                Hess[i*k+j] = Hess[j*k+i];
            }
        }
    }

    public int solve(
            double[] chebyMoments,
            double aCenter,
            double aScale

    ) {
        this.d_mus = chebyMoments;
        this.aCenter = aCenter;
        this.aScale = aScale;

        int k = d_mus.length;
        solveSetup();
        DMatrixRMaj hhMat = DMatrixRMaj.wrap(k, k, Hess);
        DMatrixRMaj gradVec = DMatrixRMaj.wrap(k, 1, grad);
        LinearSolverDense<DMatrixRMaj> solver = LinearSolverFactory_DDRM.chol(k);

        int stepCount;
        for(stepCount = 0; stepCount < maxSteps; stepCount++) {
            calcGrad();
            calcHessian();

            solver.setA(hhMat);
            solver.solve(gradVec, stepVec);
            CommonOps_DDRM.scale(-1.0, stepVec);

            for (int i = 0; i < k; i++) {
                lambdas[i] += stepVec.get(i);
            }
//            System.out.println(stepCount);
//            System.out.println(NormOps_DDRM.fastNormP2(gradVec));
//            System.out.println(Arrays.toString(lambdas));
        }

//        System.out.println(Arrays.toString(f));
        return stepCount;
    }
}

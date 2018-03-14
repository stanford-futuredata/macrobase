package msolver;

import msolver.util.MathUtil;
import org.apache.commons.math3.analysis.solvers.LaguerreSolver;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.linear.*;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.CommonOps_DDRM;
import org.ejml.dense.row.factory.DecompositionFactory_DDRM;
import org.ejml.interfaces.decomposition.EigenDecomposition_F64;

import java.util.Arrays;

// http://www.sciencedirect.com/science/article/pii/S0895717705004863#fd16
public class SimpleBoundSolver {
    private int n;  // moments from 0..2n
    private double[][] momentArray;
    private double[][] smallArray;
    private DMatrixRMaj smallMat;

    public SimpleBoundSolver(int numMoments) {
        this.n = (numMoments - 1) / 2;
        this.momentArray = new double[n+1][n+1];
        this.smallArray = new double[n][n];
        this.smallMat = new DMatrixRMaj(n, n);
    }

    /**
     * @param xs locations to calculate bound size at
     * @return total size of the error bounds provided by moments
     * http://www.personal.psu.edu/faculty/f/k/fkv/2000-06-moment-as.pdf
     */
    public double[] solveBounds(double[] moments, double[] xs) {
        for (int i = 0; i <= n; i++) {
            System.arraycopy(moments, i, momentArray[i], 0, n+1);
        }
        RealMatrix momentMatrix = new Array2DRowRealMatrix(momentArray, false);

        double[] vectorData = new double[n+1];
//        LUDecomposition momentMatrixDecomp = new LUDecomposition(momentMatrix);
        CholeskyDecomposition momentMatrixDecomp = new CholeskyDecomposition(momentMatrix);

        int numPoints = xs.length;
        double[] boundSizes = new double[numPoints];
        for (int i = 0; i < numPoints; i++) {
            double x = xs[i];
            MathUtil.calcPowers(x, vectorData);
            ArrayRealVector vec = new ArrayRealVector(vectorData, false);
            double boundSize = 1.0 / vec.dotProduct(momentMatrixDecomp.getSolver().solve(vec));
            boundSizes[i] = boundSize;
        }

        return boundSizes;
    }

    public double[] getMaxErrors(double[] moments, double[] xs, double[] ps, double[] boundSizes) {
        if (boundSizes == null) {
            throw new RuntimeException("Solve Bounds First");
        }

        int numPoints = boundSizes.length;
        double[] maxErrors = new double[numPoints];
        int n2 = moments.length;
        for (int qIdx = 0; qIdx < numPoints; qIdx++) {
            double x = xs[qIdx];
            double p = ps[qIdx];
            double maxMass = boundSizes[qIdx];
            if (n2 <= 2) {
                maxErrors[qIdx] = Math.max(p, 1.0-p);
            } else {
                double[] shiftedMoments = MathUtil.shiftPowerSum(moments, 1.0, x);
                shiftedMoments[0] -= maxMass;

                // Find Positions
                double[] positions = solvePositions(shiftedMoments);
                // TODO: why do we divide by two when coeffs are all 0?
                if (positions == null) {
                    maxErrors[qIdx] = maxMass / 2;
                    break;
                }
                int n_positive_positions = 0;
                for (int j = 0; j < n; j++) {
                    if (positions[j] > 0) n_positive_positions++;
                }
                // Special case where upper bound is 1
                if (n_positive_positions == n) {
                    maxErrors[qIdx] = Math.max(maxMass - p, p);
                } else if (n_positive_positions == 0) {
                    maxErrors[qIdx] = Math.max(1.0 - p, p - (1.0 - maxMass));
                } else {
                    double[] weights = solveWeights(shiftedMoments, positions);

                    // Compute bounds
                    double lowerBound = 0.0;
                    for (int i = 0; i < positions.length; i++) {
                        if (positions[i] < 0) {
                            lowerBound += weights[i];
                        }
                    }
                    double upperBound = lowerBound + maxMass;

                    // Return the larger one-sided error
                    maxErrors[qIdx] = Math.max(
                            Math.abs(upperBound - p),
                            Math.abs(p - lowerBound)
                    );
                }

            }
        }
        return maxErrors;
    }

    public double[] getBoundEndpoints(double[] moments, double x, double boundSize) {
        double[] bounds = new double[2];
        int n2 = moments.length;
        if (n2 <= 2) {
            bounds[0] = 0.0;
            bounds[1] = 1.0;
        } else {
            double[] shiftedMoments = MathUtil.shiftPowerSum(moments, 1.0, x);
            shiftedMoments[0] -= boundSize;

            // Find Positions
            double[] positions = solvePositions(shiftedMoments);
            if (positions == null) {
                // TODO: unclear what to do here, since we don't know p
                bounds[0] = 0.0;
                bounds[1] = 1.0;
                return bounds;
            }
            int n_positive_positions = 0;
            for (int j = 0; j < n; j++) {
                if (positions[j] > 0) n_positive_positions++;
            }

            if (n_positive_positions == n) {
                bounds[0] = 0.0;
                bounds[1] = boundSize;
            } else if (n_positive_positions == 0) {
                bounds[0] = 1.0 - boundSize;
                bounds[1] = 1.0;
            } else {
                double[] weights = solveWeights(shiftedMoments, positions);

                // Compute bounds
                double lowerBound = 0.0;
                for (int i = 0; i < positions.length; i++) {
                    if (positions[i] < 0) {
                        lowerBound += weights[i];
                    }
                }
                bounds[0] = lowerBound;
                bounds[1] = lowerBound + boundSize;
            }
        }

        return bounds;
    }

    public class CanonicalDistribution {
        public double[] positions;
        public double[] weights;
        public CanonicalDistribution(double[] positions, double[] weights) {
            this.positions = positions;
            this.weights = weights;
        }
        public double entropy() {
            return MathUtil.entropy(weights);
        }
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < positions.length; i++) {
                builder.append(String.format("(%g : %g) ", positions[i], weights[i]));
            }
            return builder.toString();
        }
    }

    public CanonicalDistribution getCanonicalDistribution(
            double[] moments
    ) {
        double[] pos = solvePositions(moments);
        double[] weights = solveWeights(moments, pos);
        return new CanonicalDistribution(pos, weights);
    }

    /**
     * The canonical distributions are minimal point-sets that match the moments.
     */
    public CanonicalDistribution[] getCanonicalDistributions(
            double[] moments,
            double[] xs
    ) {
        int n = xs.length;
        CanonicalDistribution[] results = new CanonicalDistribution[n];

        double[] maxWeights = solveBounds(moments, xs);
        for (int i = 0; i < n; i++) {
            double x = xs[i];
            double p0 = maxWeights[i];
            double[] shiftedMoments = MathUtil.shiftPowerSum(moments, 1.0, x);
            shiftedMoments[0] -= p0;

            double[] pos = solvePositions(shiftedMoments);
            double[] weights = solveWeights(shiftedMoments, pos);

            double[] fullPos = new double[pos.length+1];
            double[] fullWeights = new double[weights.length+1];
            fullPos[0] = x;
            fullWeights[0] = p0;
            for (int j = 0; j < pos.length; j++) {
                fullPos[j+1] = pos[j]+x;
                fullWeights[j+1] = weights[j];
            }
            results[i] = new CanonicalDistribution(fullPos, fullWeights);
        }
        return results;
    }

    private double[] solvePositions(double[] moments) {
        double[] coefs = orthogonalPolynomialCoefficients(moments, n);
        int deg = coefs.length - 1;
        boolean hasNonzero = false;
        for (double c : coefs) {
            if (c != 0.0) {
                hasNonzero = true;
                break;
            }
        }
        if (!hasNonzero) {
            return null;
        }
        // fill in zeros if degree too low
        while (coefs[deg] == 0.0) {
            for (int i = deg; i >= 1; i--) {
                coefs[i] = coefs[i-1];
            }
            coefs[0] = 0.0;
        }
        // rescale since the polynomial coefficients can get really small
        double scaleFactor = coefs[deg];
        for (int i = 0; i <= deg; i++) {
            coefs[i] /= scaleFactor;
        }
//        double[] positions = polynomialRoots(coefs);

        LaguerreSolver solver = new LaguerreSolver();
        Complex[] roots = solver.solveAllComplex(coefs, 0);
        double[] positions = new double[roots.length];
        for (int i = 0; i < positions.length; i++)  {
            positions[i] = roots[i].getReal();
        }
        return positions;
    }

    private double[] solveWeights(double[] moments, double[] positions) {
        for (int c = 0; c < positions.length; c++) {
            double curPow = 1.0;
            smallArray[0][c] = 1.0;
            for (int r = 1; r < positions.length; r++) {
                curPow *= positions[c];
                smallArray[r][c] = curPow;
            }
        }
        RealMatrix matrix = new Array2DRowRealMatrix(smallArray, false);
        RealVector shiftedMomentVector = new ArrayRealVector(
                Arrays.copyOf(moments, n), false
        );
        double[] weights = (new QRDecomposition(matrix)).getSolver().solve(shiftedMomentVector).toArray();
        return weights;
    }

    // Solve for polynomial roots using the companion matrix
    private double[] polynomialRoots(double[] coefs) {
        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n-1; c++) {
                smallArray[r][c] = 0.0;
                smallMat.set(r, c, 0.0);
            }
        }
        for (int i = 0; i < n-1; i++) {
            smallArray[i+1][i] = 1.0;
            smallMat.set(i+1, i, 1.0);
        }
        double a = coefs[n];
        for (int r = 0; r < n; r++) {
            smallArray[r][n-1] = -coefs[r] / a;
            smallMat.set(r, n-1, -coefs[r]/ a);
        }
        EigenDecomposition_F64<DMatrixRMaj> eigen = DecompositionFactory_DDRM.eig(n, false);
        eigen.decompose(smallMat);
        double[] eigenValues = new double[n];
        for (int i = 0; i < n; i++) {
            eigenValues[i] = eigen.getEigenvalue(i).real;
        }
        return eigenValues;

//        Array2DRowRealMatrix companionMatrix = new Array2DRowRealMatrix(smallArray, false);
//        double[] values = (new EigenDecomposition(companionMatrix)).getRealEigenvalues();
//        return values;
    }

    private double[] orthogonalPolynomialCoefficients(double[] moments, int n) {
        for (int r = 0; r < n; r++) {
            System.arraycopy(moments, r+1, smallArray[r], 0, n);
        }

        double[] coefs = new double[n+1];
        double sign = n % 2 == 0 ? 1 : -1;

        Array2DRowRealMatrix matrix = new Array2DRowRealMatrix(smallArray, false);
        for (int i = 0; i <= n; i++) {
            coefs[i] = sign * (new LUDecomposition(matrix)).getDeterminant();
            if (i == n) break;
            for (int r = 0; r < n; r++) {
                matrix.setEntry(r, i, moments[r+i]);
            }
            sign *= -1;
        }
        return coefs;
    }

}

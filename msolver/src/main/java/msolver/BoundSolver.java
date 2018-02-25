package msolver;

import org.apache.commons.math3.analysis.solvers.LaguerreSolver;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.linear.*;

import java.util.Arrays;

// http://www.sciencedirect.com/science/article/pii/S0895717705004863#fd16
public class BoundSolver {
    private double[] powerSums;
    private double min;
    private double max;
    private double midpoint;
    private double scalingFactor;
    private int n;  // We use up to the order 2n power sum
    private LUDecomposition momentMatrixDecomp;
    private RealMatrix smallMatrix;
    private RealMatrix largeMatrix;
    private boolean useLaguerreSolver = false;

    public BoundSolver(double[] powerSums, double min, double max) {
        this.powerSums = powerSums;
        this.min = min;
        this.max = max;
        this.midpoint = (max + min) / 2;
        this.scalingFactor = (max - min) / 2;
        this.n = (powerSums.length - 1) / 2;
    }

    // http://www.personal.psu.edu/faculty/f/k/fkv/2000-06-moment-as.pdf
    public double boundSizeLindsay(double est) {
        if (momentMatrixDecomp == null) {
//            double[] scaledPowerSums = MathUtil.shiftPowerSum(powerSums, scalingFactor, midpoint);
            double[] scaledPowerSums = Arrays.copyOf(powerSums, powerSums.length);
            double count = scaledPowerSums[0];
            for (int i = 0; i < scaledPowerSums.length; i++) {
                scaledPowerSums[i] /= count;
            }
            double[][] matrixData = new double[n + 1][n + 1];
            for (int r = 0; r <= n; r++) {
                System.arraycopy(scaledPowerSums, r, matrixData[r], 0, n + 1);
            }
            RealMatrix momentMatrix = new Array2DRowRealMatrix(matrixData);
            momentMatrixDecomp = new LUDecomposition(momentMatrix);
        }
        double[] vectorData = new double[n+1];
        double curPow = 1.0;
        vectorData[0] = 1.0;
//        est = (est - midpoint) / scalingFactor;
        for (int i = 1; i <= n; i++) {
            curPow *= est;
            vectorData[i] = curPow;
        }

        double[] vector2Data = momentMatrixDecomp.getSolver().solve(
                new ArrayRealVector(vectorData)).toArray();
        double dotProduct = 0.0;
        for (int i = 0; i < vectorData.length; i++) {
            dotProduct += vectorData[i] * vector2Data[i];
        }
        return 1.0 / dotProduct;
    }

    public double boundSizeRacz(double est) {
        double[] shiftedPowerSums = MathUtil.shiftPowerSum(powerSums, scalingFactor, est);
        double count = shiftedPowerSums[0];
        for (int i = 0; i < shiftedPowerSums.length; i++) {
            shiftedPowerSums[i] /= count;
        }

        // Pre-allocate matrices that will be used for this method and others
        if (smallMatrix == null) {
            smallMatrix = new Array2DRowRealMatrix(new double[n][n]);
            largeMatrix = new Array2DRowRealMatrix(new double[n + 1][n + 1]);
        }

        return maxMassAtZero(shiftedPowerSums, n);
    }

    public double quantileError(double est, double p) {
        if (powerSums.length <= 1) {
            return Math.max(p, 1.0-p);
        } else if (powerSums.length == 2) {
            return markovBoundError(est, p);
        }

        // Pre-allocate matrices that will be used for this method and others
        if (smallMatrix == null) {
            smallMatrix = new Array2DRowRealMatrix(new double[n][n]);
            largeMatrix = new Array2DRowRealMatrix(new double[n + 1][n + 1]);
        }

        double[] shiftedPowerSums = MathUtil.shiftPowerSum(powerSums, scalingFactor, est);
        double count = shiftedPowerSums[0];
        for (int i = 0; i < shiftedPowerSums.length; i++) {
            shiftedPowerSums[i] /= count;
        }
        double massAtZero = maxMassAtZero(shiftedPowerSums, n);
        shiftedPowerSums[0] -= massAtZero;

        // Find positions
        double[] coefs = orthogonalPolynomialCoefficients(shiftedPowerSums, n);
        boolean coefsAllZeros = true;
        for (double c : coefs) {
            coefsAllZeros &= (c == 0);
        }
        if (coefsAllZeros) {
            return massAtZero / 2.0;  // TODO: does coefs all 0 imply symmetric error?
        }

        double[] positions;
        if (useLaguerreSolver) {
            LaguerreSolver rootSolver = new LaguerreSolver();
            Complex[] roots = rootSolver.solveAllComplex(coefs, 0.0);
            positions = new double[roots.length];
            for (int i = 0; i < roots.length; i++) {
                positions[i] = roots[i].getReal();
            }
        } else {
            positions = polynomialRoots(coefs);
        }

        int n_positive_positions = 0;
        for (int i = 0; i < n; i++) {
            if (positions[i] > 0) n_positive_positions++;
        }

        // Special case where upper bound is 1
        if (n_positive_positions == n) {
            return Math.max(massAtZero - p, p);
        }
        // Special case where lower bound is 0
        if (n_positive_positions == 0) {
            return Math.max(1.0 - p, p - (1.0 - massAtZero));
        }

        // Find weights
        RealMatrix matrix = smallMatrix;
        for (int c = 0; c < positions.length; c++) {
            double curPow = 1.0;
            matrix.setEntry(0, c,1.0);
            for (int r = 1; r < positions.length; r++) {
                curPow *= positions[c];
                matrix.setEntry(r, c, curPow);
            }
        }

        double[] weights = (new LUDecomposition(matrix)).getSolver().solve(
                new ArrayRealVector(Arrays.copyOfRange(shiftedPowerSums, 0, n))).toArray();

        // Compute bounds
        double lowerBound = 0.0;
        for (int i = 0; i < positions.length; i++) {
            if (positions[i] < 0) {
                lowerBound += weights[i];
            }
        }
        double upperBound = lowerBound + massAtZero;

        // Return the larger one-sided error
        return Math.max(
                Math.abs(upperBound - p),
                Math.abs(p - lowerBound)
        );
    }

    // Solve for polynomial roots using the companion matrix
    private double[] polynomialRoots(double[] coefs) {
        RealMatrix companionMatrix = smallMatrix;
        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n-1; c++) {
                companionMatrix.setEntry(r, c, 0.0);
            }
        }
        for (int i = 0; i < n-1; i++) {
            companionMatrix.setEntry(i+1, i, 1.0);
        }
        double a = coefs[n];
        for (int r = 0; r < n; r++) {
            companionMatrix.setEntry(r, n-1, -coefs[r] / a);
        }
        return (new EigenDecomposition(companionMatrix)).getRealEigenvalues();
    }

    private double maxMassAtZero(double[] shiftedPowerSums, int n) {
        RealMatrix numeratorMatrix = largeMatrix;
        for (int r = 0; r <= n; r++) {
            for (int c = 0; c <= n; c++) {
                numeratorMatrix.setEntry(r, c, shiftedPowerSums[r+c]);
            }
        }

        RealMatrix denominatorMatrix = smallMatrix;
        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n; c++) {
                denominatorMatrix.setEntry(r, c, shiftedPowerSums[r+c+2]);
            }
        }

        return (new LUDecomposition(numeratorMatrix)).getDeterminant() /
                (new LUDecomposition(denominatorMatrix)).getDeterminant();
    }

    private double[] orthogonalPolynomialCoefficients(double[] shiftedPowerSums, int n) {
        RealMatrix matrix = smallMatrix;
        for (int r = 0; r < n; r++) {
            for (int c = 0; c < n; c++) {
                matrix.setEntry(r, c, shiftedPowerSums[r+c+1]);
            }
        }

        double[] coefs = new double[n+1];
        double sign = n % 2 == 0 ? 1 : -1;

        for (int i = 0; i <= n; i++) {
            coefs[i] = sign * (new LUDecomposition(matrix)).getDeterminant();
            if (i == n) break;
            for (int r = 0; r < n; r++) {
                matrix.setEntry(r, i, shiftedPowerSums[r+i]);
            }
            sign *= -1;
        }
        return coefs;
    }

    private double markovBoundError(double estimate, double p) {
        double mean = powerSums[1] / powerSums[0];
        double lowerBound = Math.max(0.0, 1 - (mean - min) / (estimate - min));
        double upperBound = Math.min(1.0, (max - mean) / (max - estimate));
        return Math.max(upperBound - p, p - lowerBound);
    }

    public void setUseLaguerreSolver(boolean useLaguerreSolver) {
        this.useLaguerreSolver = useLaguerreSolver;
    }


    /* Experimental code */

    // http://www.academia.edu/24934478/Bounds_on_the_Tail_Probability_and_Absolute_Difference_Between_Two_Distributions
    public double quantileErrorGoria() {
        ChebyshevMomentSolver solverK = ChebyshevMomentSolver.fromPowerSums(min, max, powerSums);
        double[] chebyshevMoments = solverK.getChebyshevMoments();
        ChebyshevMomentSolver solverKm1 = new ChebyshevMomentSolver(
                Arrays.copyOf(chebyshevMoments, chebyshevMoments.length - 1));
        ChebyshevMomentSolver solverKm2 = new ChebyshevMomentSolver(
                Arrays.copyOf(chebyshevMoments, chebyshevMoments.length - 2));
        solverK.solve(1e-10);
        solverKm1.solve(1e-10);
        solverKm2.solve(1e-10);
        double entropyK = computeContinuousEntropy(solverK.getLambdas(), chebyshevMoments);
        double entropyKm1 = computeContinuousEntropy(solverKm1.getLambdas(), chebyshevMoments);
        double entropyKm2 = computeContinuousEntropy(solverKm2.getLambdas(), chebyshevMoments);
        double entropyDelta = Math.pow(entropyK - entropyKm1, 2) / (entropyK - 2 * entropyKm1 + entropyKm2);
        return 3 * Math.sqrt(-1 + Math.sqrt(1 + 4/9.0 * entropyDelta));
    }

//    public void printEntropies() {
//        ChebyshevMomentSolver solverK = ChebyshevMomentSolver.fromPowerSums(min, max, powerSums);
//        double[] chebyshevMoments = solverK.getChebyshevMoments();
//        for (int i = 2; i <= chebyshevMoments.length; i++) {
//            ChebyshevMomentSolver solver = new ChebyshevMomentSolver(Arrays.copyOf(chebyshevMoments, i));
//            solver.solve(1e-10);
//            System.out.println(computeContinuousEntropy(solver.getLambdas(), chebyshevMoments));
//        }
//    }

    private double computeContinuousEntropy(double[] lambdas, double[] chebyshevMoments) {
        double entropy = 0.0;
        for (int i = 0; i < lambdas.length; i++) {
            entropy += lambdas[i] * chebyshevMoments[i];
        }
        return entropy;
    }

//    public double computeDiscreteEntropy(double[] data) {
//        HashMap<Double, Double> probMap = new HashMap<Double, Double>();
//        for (double value : data) {
//            Double tmpKey = value;
//            Double tmpValue = probMap.remove(tmpKey);
//            if (tmpValue == null) {
//                probMap.put(tmpKey, 1.0);
//            } else {
//                probMap.put(tmpKey, tmpValue + 1.0);
//            }
//        }
//        for (Entry<Double, Double> e : probMap.entrySet()) {
//            probMap.put(e.getKey(), e.getValue() / data.length);
//        }
//        double entropy = 0.0;
//        for (Double prob : probMap.values()) {
//            entropy -= prob * Math.log(prob);
//        }
//        return entropy;
//    }
}

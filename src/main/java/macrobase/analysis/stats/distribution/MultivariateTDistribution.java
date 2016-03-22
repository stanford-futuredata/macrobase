package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultivariateTDistribution {
    private static final Logger log = LoggerFactory.getLogger(MultivariateTDistribution.class);
    private RealVector mean;
    private RealMatrix covarianceMatrix;
    private RealMatrix precisionMatrix;
    private double degreesOfFreedom;

    private int dimensions;
    private double multiplier;

    public MultivariateTDistribution(RealVector mean, RealMatrix covarianceMatrix, int degreesOfFreedom) {
        this.mean = mean;
        this.covarianceMatrix = covarianceMatrix;
        if (mean.getDimension() > 1) {
            this.precisionMatrix = MatrixUtils.blockInverse(covarianceMatrix, (-1 + covarianceMatrix.getColumnDimension()) / 2);
        } else {
            this.precisionMatrix = MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(1. / covarianceMatrix.getEntry(0, 0));
        }
        this.degreesOfFreedom = degreesOfFreedom;

        this.dimensions = mean.getDimension();

        double determinant = new LUDecomposition(covarianceMatrix).getDeterminant();

        this.multiplier = halfGammaRatio(dimensions + degreesOfFreedom, degreesOfFreedom)/
                Math.pow(Math.PI * degreesOfFreedom, 0.5 * dimensions) /
                Math.pow(determinant, 0.5);
    }


    public double density(RealVector vector) {
        RealVector _diff = vector.subtract(mean);
        double prob = 1./degreesOfFreedom * _diff.dotProduct(precisionMatrix.operate(_diff));
        return multiplier * Math.pow(1 + prob, -(degreesOfFreedom + dimensions) / 2);
    }

    public static double halfGammaRatio(int doubleNumerator, int doubleDenominator) {
        double gammaRation = 1;
        for (int i = 2; i < doubleNumerator - doubleDenominator + 1; i+=2) {
            gammaRation *= 0.5 * (doubleNumerator - i);
        }
        // using the that Gamma(n+1/2) / Gamma(n) ~= sqrt(n)
        // http://mathworld.wolfram.com/images/equations/GammaFunction/NumberedEquation29.gif
        if ((doubleNumerator - doubleDenominator) % 2 == 1) {
            double j = 0.5 * doubleDenominator;
            gammaRation *= Math.sqrt(j) *
                    ( 1 - 1. / (8 * j) + 1. / (128 * j * j) + 5. / (1024 * j * j * j));
        }
        return gammaRation;
    }
}

package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.special.Gamma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultivariateTDistribution implements MultivariateDistribution{
    private static final Logger log = LoggerFactory.getLogger(MultivariateTDistribution.class);
    private RealVector mean;
    private RealMatrix precisionMatrix;
    private double dof;

    private int D;
    private double multiplier;

    public MultivariateTDistribution(RealVector mean, RealMatrix covarianceMatrix, double degreesOfFreedom) {
        this.mean = mean;
        if (mean.getDimension() > 1) {
            this.precisionMatrix = MatrixUtils.blockInverse(covarianceMatrix, (-1 + covarianceMatrix.getColumnDimension()) / 2);
        } else {
            this.precisionMatrix = MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(1. / covarianceMatrix.getEntry(0, 0));
        }
        this.dof = degreesOfFreedom;

        this.D = mean.getDimension();

        double determinant = new LUDecomposition(covarianceMatrix).getDeterminant();

        this.multiplier = Math.exp(Gamma.logGamma(0.5 * (D + dof)) - Gamma.logGamma(0.5 * dof)) /
                Math.pow(Math.PI * dof, 0.5 * D) /
                Math.pow(determinant, 0.5);
    }

    public double density(RealVector vector) {
        if (dof == 0) {
            return 0;
        }
        RealVector _diff = vector.subtract(mean);
        double prob = 1. / dof * _diff.dotProduct(precisionMatrix.operate(_diff));
        return multiplier * Math.pow(1 + prob, -(dof + D) / 2);
    }
}

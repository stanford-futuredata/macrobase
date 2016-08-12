package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

/**
 * Wrapper around MultivariateNormalDistribution that operates with RealVector and RealMatrix
 */
public class MultivariateNormal implements MultivariateDistribution {
    private MultivariateNormalDistribution distribution;

    public MultivariateNormal(RealVector mean, RealMatrix sigma) {
        double[][] arrayOfMatrix = new double[sigma.getColumnDimension()][sigma.getRowDimension()];
        for (int i = 0; i < sigma.getColumnDimension(); i++) {
            arrayOfMatrix[i] = sigma.getRow(i);
        }
        distribution = new MultivariateNormalDistribution(mean.toArray(), arrayOfMatrix);
    }

    public double density(RealVector vector) {
        return distribution.density(vector.toArray());
    }

    public MultivariateNormalDistribution getDistribution() {
        return distribution;
    }
}

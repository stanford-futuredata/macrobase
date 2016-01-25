package macrobase.analysis.outlier;

import macrobase.datamodel.Datum;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KDE extends OutlierDetector {

    public enum Kernel {
        EPANECHNIKOV_MULTIPLICATIVE;

        public double density(RealVector u) {
            switch (this) {
                case EPANECHNIKOV_MULTIPLICATIVE:
                    double rtn = 1.0;
                    final int d = u.getDimension();
                    for (int i = 0; i < d; i++) {
                        double i2 = u.getEntry(i) * u.getEntry(i);
                        if (i2 > 1) {
                            return 0;
                        }
                        rtn *= 1 - i2;
                    }
                    return Math.pow(0.75, d) * rtn;
                default:
                    throw new RuntimeException("No kernel implemented");
            }
        }

        public double norm(int dimension) {
            switch (this) {
                case EPANECHNIKOV_MULTIPLICATIVE:
                    return Math.pow(0.6, dimension);
                default:
                    throw new RuntimeException("No norm implemented for this kernel");
            }
        }
    }

    private Kernel kernel;
    private List<Datum> densityPopulation;
    private RealMatrix bandwidth; // symmetric and positive definite
    private RealMatrix bandwidthToNegativeHalf;
    private double scoreScalingFactor;

    public KDE(Kernel kernel, RealMatrix bandwidth) {
        this.kernel = kernel;
        this.bandwidth = bandwidth;
        RealMatrix inverseBandwidth = MatrixUtils.blockInverse(bandwidth, (bandwidth.getColumnDimension() - 1 )/2);
        this.bandwidthToNegativeHalf = (new EigenDecomposition(inverseBandwidth)).getSquareRoot();
    }

    @Override
    public void train(List<Datum> data) {
        // Very rudimentary sampling, write something better in the future.
        densityPopulation = new ArrayList<Datum>(data);
        Collections.shuffle(densityPopulation);
        double bandwidthDeterminantSqrt = Math.sqrt((new EigenDecomposition(bandwidth)).getDeterminant());

        // pick 1% of the data (1/100 is a randomly chosen number)
        this.densityPopulation = densityPopulation.subList(0, (int) (0.01 * densityPopulation.size()));
        this.scoreScalingFactor = 1.0 / (bandwidthDeterminantSqrt * densityPopulation.size());
    }

    @Override
    public double score(Datum datum) {
        double _score = 0.0;
        for(int i = 0 ; i < densityPopulation.size(); i++) {
            RealVector difference = datum.getMetrics().subtract(densityPopulation.get(i).getMetrics());
            double _diff = kernel.density(this.bandwidthToNegativeHalf.operate(difference));
            _score += _diff;
        }
        return _score * this.scoreScalingFactor;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        throw new RuntimeException("ZScore equivalence is not implemented yet.");
    }

}

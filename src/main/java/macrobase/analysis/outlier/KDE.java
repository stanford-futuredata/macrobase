package macrobase.analysis.outlier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

public class KDE extends OutlierDetector {

    public enum Bandwidth {
        NORMAL_SCALE,
        MANUAL
    }

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

        public double secondMoment(int dimension) {
            switch (this) {
                case EPANECHNIKOV_MULTIPLICATIVE:
                    return Math.pow(0.2, dimension);
                default:
                    throw new RuntimeException("No second moment stored for this kernel");
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
    private double[] allScores;
    private Bandwidth bandwidthType;
    private double proportionOfDataToUse;

    public KDE(Kernel kernel, Bandwidth bandwidthType) {
        this.kernel = kernel;
        this.bandwidthType = bandwidthType;
        // Pick 1 % of the data, randomly
        this.proportionOfDataToUse = 0.01;
    }

    /**
     * Manually set bandwidth of KDE
     * @param bandwidth
     */
    public void setBandwidth(RealMatrix bandwidth) {
        this.bandwidth = bandwidth;
        calculateBandwidthAncillaries();
    }

    /**
     * Calculates bandwidth matrix based on the data that KDE should run on
     * @param data
     */
    private void setBandwidth(List<Datum> data) {
        final int metricsDimensions = data.get(0).getMetrics().getDimension();
        RealMatrix bandwidth = MatrixUtils.createRealIdentityMatrix(metricsDimensions);
        // double matrix_scale = 0.1;  // Scale identity matrix by this much
        // bandwidth = bandwidth.scalarMultiply(matrix_scale);
        switch (bandwidthType) {
            case NORMAL_SCALE:
                final double standardNormalQunatileDifference = 1.349;
                int dimensions = data.get(0).getMetrics().getDimension();
                for (int d=0; d<dimensions; d++) {
                    int size = data.size();
                    double[] dataIn1D = new double[size];
                    for (int i=0; i<size; i++) {
                        dataIn1D[i] = data.get(i).getMetrics().getEntry(d);
                    }
                    Percentile quantile = new Percentile();
                    final double twentyfive = quantile.evaluate(dataIn1D, 25);
                    final double seventyfive = quantile.evaluate(dataIn1D, 75);
                    final double interQuantileDeviation = (seventyfive - twentyfive) / standardNormalQunatileDifference;
                    final double constNumerator = 8 * Math.pow(Math.PI, 0.5) * kernel.norm(1);
                    final double constDenominator = 3 * Math.pow(kernel.secondMoment(1), 2) * data.size() * this.proportionOfDataToUse;
                    double dimensional_bandwidth = Math.pow(constNumerator / constDenominator, 0.2) * interQuantileDeviation;
                    bandwidth.setEntry(d, d, dimensional_bandwidth);
                }
                break;
            case MANUAL:
                break;
        }

        if (bandwidthType != Bandwidth.MANUAL) {
            this.bandwidth = bandwidth;
            this.calculateBandwidthAncillaries();
        }
    }

    private void calculateBandwidthAncillaries() {
        RealMatrix inverseBandwidth;
        if (bandwidth.getColumnDimension() > 1) {
            inverseBandwidth = MatrixUtils.blockInverse(bandwidth, (bandwidth.getColumnDimension() - 1) / 2);
        } else {
            // Manually invert size 1 x 1 matrix, because block Inverse requires dimensions > 1
            inverseBandwidth = bandwidth.copy();
            inverseBandwidth.setEntry(0, 0, 1.0/inverseBandwidth.getEntry(0, 0));
        }
        this.bandwidthToNegativeHalf = (new EigenDecomposition(inverseBandwidth)).getSquareRoot();

    }

    @Override
    public void train(List<Datum> data) {
        this.setBandwidth(data);

        // Very rudimentary sampling, write something better in the future.
        densityPopulation = new ArrayList<Datum>(data);
        Collections.shuffle(densityPopulation);
        double bandwidthDeterminantSqrt = Math.sqrt((new EigenDecomposition(bandwidth)).getDeterminant());

        this.densityPopulation = densityPopulation.subList(0, (int) (this.proportionOfDataToUse * densityPopulation.size()));
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
        return - _score * this.scoreScalingFactor;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        throw new RuntimeException("ZScore equivalence is not implemented yet.");
    }


}

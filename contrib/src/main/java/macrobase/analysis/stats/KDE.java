package macrobase.analysis.stats;

import macrobase.analysis.stats.kernel.EpanchnikovMulticativeKernel;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class KDE extends BatchTrainScore {

    // KDE defaults
    public static final Double KDE_BANDWIDTH_MULTIPLIER_DEFAULT = 1.0;
    public static final BandwidthAlgorithm KDE_BANDWIDTH_ALGORITHM_DEFAULT = KDE.BandwidthAlgorithm.OVERSMOOTHED;
    public static final KernelType KDE_KERNEL_TYPE_DEFAULT = KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE;

    // Algorithm to use when choosing the bandwidth for the given data.
    public static final String KDE_BANDWIDTH_ALGORITHM = "macrobase.analysis.kde.bandwidthAlgorithm";
    public static final String KDE_PROPORTION_OF_DATA_TO_USE = "macrobase.analysis.kde.proportionOfDataToUse";

    // Multiplies the bandwidth that was gotten algorithmically by this given constant (double).
    public static final String KDE_BANDWIDTH_MULTIPLIER = "macrobase.analysis.kde.bandwidthMultiplier";
    public static final String KDE_KERNEL_TYPE = "macrobase.analysis.kde.kernelType";

    public KDE.BandwidthAlgorithm getKDEBandwidth(MacroBaseConf conf) throws ConfigurationException {
        if (!conf.isSet(KDE_BANDWIDTH_ALGORITHM)) {
            return KDE_BANDWIDTH_ALGORITHM_DEFAULT;
        }
        return KDE.BandwidthAlgorithm.valueOf(conf.getString(KDE_BANDWIDTH_ALGORITHM));
    }

    public KDE.KernelType getKDEKernelType(MacroBaseConf conf) throws ConfigurationException {
        if (!conf.isSet(KDE_KERNEL_TYPE)) {
            return KDE_KERNEL_TYPE_DEFAULT;
        }
        return KDE.KernelType.valueOf(conf.getString(KDE_KERNEL_TYPE));
    }

    private static final Logger log = LoggerFactory.getLogger(KDE.class);
    protected double bandwidthDeterminantSqrt;
    protected KernelType kernelType;
    protected macrobase.analysis.stats.kernel.Kernel kernel;
    private List<Datum> densityPopulation;
    protected RealMatrix bandwidth; // symmetric and positive definite
    protected RealMatrix bandwidthToNegativeHalf;
    protected double scoreScalingFactor;
    private double[] allScores;
    private BandwidthAlgorithm bandwidthAlgorithm;
    protected double proportionOfDataToUse;
    protected double algorithmicBandwidthMultiplier = 1.0;

    private final Random random;
    protected int metricsDimensions;

    public enum BandwidthAlgorithm {
        NORMAL_SCALE,
        OVERSMOOTHED,
        MANUAL
    }

    public enum KernelType {
        EPANECHNIKOV_MULTIPLICATIVE;

        public macrobase.analysis.stats.kernel.Kernel constructKernel(int dimensions) {
            switch (this) {
                case EPANECHNIKOV_MULTIPLICATIVE:
                    return new EpanchnikovMulticativeKernel(dimensions);
                default:
                    throw new RuntimeException("Unexpected Kernel given");
            }
        }
    }

    public KDE(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
        this.random = conf.getRandom();
        this.kernelType = getKDEKernelType(conf);
        this.bandwidthAlgorithm = getKDEBandwidth(conf);
        log.debug("using {} bandwidth selection algorithm", this.bandwidthAlgorithm);
        this.algorithmicBandwidthMultiplier = conf.getDouble(KDE_BANDWIDTH_MULTIPLIER, KDE_BANDWIDTH_MULTIPLIER_DEFAULT);
        
        this.kernel = this.kernelType.constructKernel(this.metricsDimensions);
        // Pick 1 % of the data, randomly
        this.proportionOfDataToUse = 0.01;
    }

    public void setProportionOfDataToUse(double ratio) {
        this.proportionOfDataToUse = ratio;
    }

    /**
     * Scaled version of the kernel (K_H in the literature)
     * @param vector
     * @return
     */
    protected double scaledKernelDensity(RealVector vector) {
        return this.kernel.density(this.bandwidthToNegativeHalf.operate(vector));
    }

    /**
     * Manually set bandwidth of KDE
     *
     * @param bandwidth
     */
    public void setBandwidth(RealMatrix bandwidth) {
        log.trace("Given bandwidth matrix: {}", bandwidth);
        this.bandwidth = bandwidth;
        calculateBandwidthAncillaries();
    }

    /**
     * Calculates bandwidth matrix based on the data that KDE should run on
     *
     * @param data
     */
    protected void setBandwidth(List<Datum> data) {
        this.metricsDimensions = data.get(0).metrics().getDimension();
        RealMatrix bandwidth = MatrixUtils.createRealIdentityMatrix(metricsDimensions);
        log.debug("running with bandwidthAlgorithm: {}", bandwidthAlgorithm);
        switch (bandwidthAlgorithm) {
            case NORMAL_SCALE:
                final double standardNormalQunatileDifference = 1.349;
                for (int d = 0; d < this.metricsDimensions; d++) {
                    int size = data.size();
                    double[] dataIn1D = new double[size];
                    for (int i = 0; i < size; i++) {
                        dataIn1D[i] = data.get(i).metrics().getEntry(d);
                    }
                    Percentile quantile = new Percentile();
                    final double twentyfive = quantile.evaluate(dataIn1D, 25);
                    final double seventyfive = quantile.evaluate(dataIn1D, 75);
                    final double interQuantileDeviation = (seventyfive - twentyfive) / standardNormalQunatileDifference;
                    final double constNumerator = 8 * Math.pow(Math.PI, 0.5) * kernel.norm1D();
                    final double constDenominator = 3 * Math.pow(kernel.secondMoment1D(),
                                                                 2) * data.size() * this.proportionOfDataToUse;
                    double dimensional_bandwidth = Math.pow(constNumerator / constDenominator,
                                                            0.2) * interQuantileDeviation;
                    bandwidth.setEntry(d, d, dimensional_bandwidth);
                }
                break;
            case OVERSMOOTHED:
                final double constNumerator = 8 * Math.pow(Math.PI, 0.5) * kernel.norm1D();
                final double constDenominator = 3 * Math.pow(kernel.secondMoment1D(),
                                                             2) * data.size() * this.proportionOfDataToUse;
                final double covarianceScale = Math.pow(constNumerator / constDenominator, 0.2);
                log.info("covariance Scale: {}", covarianceScale);
                RealMatrix covariance = Covariance.getCovariance(data);
                log.info("covariance of the data is: {}", covariance);
                bandwidth = covariance.scalarMultiply(covarianceScale);
            case MANUAL:
                break;
        }

        if (bandwidthAlgorithm != BandwidthAlgorithm.MANUAL) {
            bandwidth = bandwidth.scalarMultiply(this.algorithmicBandwidthMultiplier);
            this.setBandwidth(bandwidth);
        }
    }

    private void calculateBandwidthAncillaries() {
        RealMatrix inverseBandwidth;
        if (bandwidth.getColumnDimension() > 1) {
            inverseBandwidth = MatrixUtils.blockInverse(bandwidth, (bandwidth.getColumnDimension() - 1) / 2);
        } else {
            // Manually invert size 1 x 1 matrix, because block Inverse requires dimensions > 1
            inverseBandwidth = bandwidth.copy();
            inverseBandwidth.setEntry(0, 0, 1.0 / inverseBandwidth.getEntry(0, 0));
        }
        this.bandwidthToNegativeHalf = (new EigenDecomposition(inverseBandwidth)).getSquareRoot();
        this.bandwidthDeterminantSqrt = Math.sqrt((new EigenDecomposition(bandwidth)).getDeterminant());
    }

    @Override
    public void train(List<Datum> data) {
        this.setBandwidth(data);

        log.debug("training on {}% of the data", 100 * this.proportionOfDataToUse);

        // Very rudimentary sampling, write something better in the future.
        densityPopulation = new ArrayList<>(data);
        Collections.shuffle(densityPopulation, random);

        this.densityPopulation = densityPopulation.subList(0,
                                                           (int) (this.proportionOfDataToUse * densityPopulation.size()));
        this.scoreScalingFactor = 1.0 / (bandwidthDeterminantSqrt * densityPopulation.size());
    }

    @Override
    public double score(Datum datum) {
        double _score = 0.0;
        for (int i = 0; i < densityPopulation.size(); i++) {
            RealVector difference = datum.metrics().subtract(densityPopulation.get(i).metrics());
            double _diff = scaledKernelDensity(difference);
            _score += _diff;
        }
        return -_score * this.scoreScalingFactor;
    }
}

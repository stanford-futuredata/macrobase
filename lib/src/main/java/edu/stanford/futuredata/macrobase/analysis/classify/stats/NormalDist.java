package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import org.apache.commons.math3.util.FastMath;

/**
 * Implements a look-up table in order to quickly obtain the CDF of a
 * normal distribution.
 */
public class NormalDist {
    public static final double GRANULARITY = 0.1;
    public static final double MAXZSCORE = 3.5;
    public static final double MINZSCORE = -3.5;
    public static final double LUT_OFFSET = -MINZSCORE/GRANULARITY;
    public static final double[] CDF_LUT = new double[]{
            0.000233, 0.000337, 0.000483, 0.000687, 0.000968,
            0.001350, 0.001866, 0.002555, 0.003467, 0.004661,
            0.006210, 0.008198, 0.010724, 0.013903, 0.017864,
            0.022750, 0.028717, 0.035930, 0.044565, 0.054799,
            0.066807, 0.080757, 0.096800, 0.115070, 0.135666,
            0.158655, 0.184060, 0.211855, 0.241964, 0.274253,
            0.308538, 0.344578, 0.382089, 0.420740, 0.460172,
            0.500000,
            0.539828, 0.579260, 0.617911, 0.655422, 0.691462,
            0.725747, 0.758036, 0.788145, 0.815940, 0.841345,
            0.864334, 0.884930, 0.903200, 0.919243, 0.933193,
            0.945201, 0.955435, 0.964070, 0.971283, 0.977250,
            0.982136, 0.986097, 0.989276, 0.991802, 0.993790,
            0.995339, 0.996533, 0.997445, 0.998134, 0.998650,
            0.999032, 0.999313, 0.999517, 0.999663, 0.999767
    };

    public NormalDist() { }

    public double cdf(double mean, double std, double x) {
        double zscore = (x - mean) / std;

        if (zscore > MAXZSCORE) {
            return 1.0;
        }
        if (zscore < MINZSCORE) {
            return 0.0;
        }

        // Interpolate the CDF
        double exactEntry = zscore / GRANULARITY + LUT_OFFSET;
        int lowerEntry = (int) FastMath.floor(exactEntry);
        int higherEntry = (int) FastMath.ceil(exactEntry);

        if (lowerEntry == higherEntry) {
            return CDF_LUT[lowerEntry];
        } else {
            return CDF_LUT[lowerEntry] +
                    ((exactEntry - lowerEntry) * (CDF_LUT[higherEntry] - CDF_LUT[lowerEntry]));
        }
    }
}

package macrobase.analysis.stats;

import java.util.Arrays;
import java.util.List;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BinnedKDE is a KDE with scores being calculated using binned approximation.
 * Only supports diagonal bandwidth matrices (It will not crash, but it might not be accurate)
 */
public class BinnedKDE extends KDE {

    private static final Logger log = LoggerFactory.getLogger(BinnedKDE.class);
    private double[][] bins;
    private double[][] kernelWeights;
    private double[][] densityEstimates;
    private double[] minimums;
    private double[] maximums;
    private int numBins;
    private int numIntervals;

    private int L;
    private double delta;

    public BinnedKDE(MacroBaseConf conf) {
        super(conf);
        proportionOfDataToUse = 1.0;
        this.numBins = conf.getInt(MacroBaseConf.BINNED_KDE_BINS, MacroBaseDefaults.BINNED_KDE_BINS);
        this.numIntervals = this.numBins - 1;
    }

    @Override
    public void train(List<Datum> data) {
        this.setBandwidth(data);
        log.debug("training BinnedKDE");
        assert this.metricsDimensions == 1;
        this.linearAssignToBins(data);
        this.calculateKernelWeights(data);
    }


    private void calculateKernelWeights(List<Datum> data) {
        // TODO: now only supports 1D!!!!
        kernelWeights = new double[metricsDimensions][numBins];

        double binsThatMatter = kernel.effectTiveSupportWidth1D() * Math.sqrt(
                this.bandwidth.getEntry(0, 0)) * this.numIntervals / (this.maximums[0] - this.minimums[0]);
        log.debug("binsThatMatter: {}", binsThatMatter);
        this.L = Math.min((int) binsThatMatter, this.numIntervals);
        log.debug("GOT L= {}", L);

        final double h = Math.sqrt(this.bandwidth.getEntry(0, 0));
        double scalingFactor = 1.0 / (data.size() * h);
        final double constant = (this.maximums[0] - this.minimums[0]) / (this.numIntervals * h);
        double[] array = new double[1];
        for (int l = 0; l < numIntervals; l++) {
            array[0] = constant * l;
            RealVector vector = new ArrayRealVector(array);
            kernelWeights[0][l] = scalingFactor * this.kernel.density(vector);
        }


        densityEstimates = new double[metricsDimensions][numBins];
        for (int d = 0; d < 1; d++) {
            for (int j = 0; j < numBins; ++j) {
                for (int l = -this.L; l <= this.L; l++) {
                    int binIndex = j - l;
                    if (binIndex < 0 || binIndex >= numBins) {
                        continue;
                    }
                    densityEstimates[d][j] += bins[d][binIndex] * kernelWeights[d][Math.abs(l)];
                }
            }
        }
    }

    @Override
    public double score(Datum datum) {
        // TODO: now only supports 1D datum
        for (int d = 0; d < 1; d++) {
            double pointValue = datum.getMetrics().getEntry(d);
            double binDouble = (pointValue - this.minimums[d]) / delta;
            return -densityEstimates[d][(int) binDouble];
        }
        return 0;
    }

    /**
     * Assigns data to bins (in all dimensions) using linear binning
     *
     * @param data
     */
    private void linearAssignToBins(List<Datum> data) {
        this.bins = new double[metricsDimensions][numBins];
        this.minimums = new double[metricsDimensions];
        this.maximums = new double[metricsDimensions];
        for (int d = 0; d < this.metricsDimensions; ++d) {
            int size = data.size();
            double[] dataIn1D = new double[size];
            for (int i = 0; i < size; i++) {
                dataIn1D[i] = data.get(i).getMetrics().getEntry(d);
            }

            Arrays.sort(dataIn1D);
            this.minimums[d] = dataIn1D[0];
            this.maximums[d] = dataIn1D[size - 1];
            this.delta = (this.maximums[d] - this.minimums[d]) / numIntervals;

            for (int i = 0; i < size; i++) {
                double pointValue = data.get(i).getMetrics().getEntry(d);
                double binDouble = (pointValue - this.minimums[d]) / delta;
                int lowerBin = (int) binDouble;
                // Assign weights to lower and upper bins linearly proportional to distances
                this.bins[d][lowerBin] += binDouble - lowerBin;
                this.bins[d][Math.min(lowerBin + 1, numBins - 1)] += 1 - (binDouble - lowerBin);
            }
        }
    }
}

package macrobase.analysis.outlier;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.Collections;
import java.util.List;

/**
 * BinnedKDE is a KDE with scores being calculated using binned approximation.
 * Only supports diagonal bandwidth matrices (It will not crash, but it might not be accurate)
 */
public class BinnedKDE extends KDE {
    private double[][] bins;
    private double[][] kernelWeights;  // k_l which is proportional to the value of kernel at the bins
    private double[] minimums;
    private double[] maximums;
    private final int numIntervals = 1000;
    private final int numBins = numIntervals + 1;

    public BinnedKDE(KernelType kernelType, Bandwidth bandwidthType) {
        super(kernelType, bandwidthType);
    }

    @Override
    public void train(List<Datum> data) {
        this.setBandwidth(data);
        this.linearAssignToBins(data);
        // FIXME L = Math.min((int)this.band)
        this.calculateKernelWeights(data);
    }

    private void calculateKernelWeights(List<Datum> data) {
        for (int i = 0; i < numBins; i++) {
            //kernelWeights[i] =
        }
    }

    /**
     * Assigns data to bins (in all dimensions) using linear binning
     * @param data
     */
    private void linearAssignToBins(List<Datum> data) {
        this.bins = new double[metricsDimensions][numBins];
        this.minimums = new double[metricsDimensions];
        this.maximums = new double[metricsDimensions];
        for (int d=0; d < this.metricsDimensions; ++d) {
            int size = data.size();
            double[] dataIn1D = new double[size];
            for (int i=0; i<size; i++) {
                dataIn1D[i] = data.get(i).getMetrics().getEntry(d);
            }

            Percentile quantile = new Percentile();
            this.minimums[d] = quantile.evaluate(dataIn1D, 0);
            this.maximums[d] = quantile.evaluate(dataIn1D, 100);
            final double delta = (this.maximums[d] - this.minimums[d]) / numIntervals;

            for (int i=0; i<size; i++) {
                double pointValue = data.get(i).getMetrics().getEntry(d);
                double binDouble = (pointValue - this.minimums[d]) / delta;
                int lowerBin = (int)binDouble;
                // Assign weights to lower and apper bins linearly proportional to distances
                this.bins[d][lowerBin] += binDouble - lowerBin;
                this.bins[d][lowerBin + 1] += 1 - (binDouble - lowerBin);
            }
        }
    }
}

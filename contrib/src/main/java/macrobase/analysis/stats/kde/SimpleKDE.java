package macrobase.analysis.stats.kde;

import macrobase.analysis.stats.kde.kernel.BandwidthSelector;
import macrobase.analysis.stats.kde.kernel.GaussianKernel;
import macrobase.analysis.stats.kde.kernel.Kernel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SimpleKDE {
    private static final Logger log = LoggerFactory.getLogger(SimpleKDE.class);

    private BandwidthSelector bwSelector;

    private double[] bandwidth;
    private Kernel kernel;
    private boolean ignoreSelf = false;

    private List<double[]> trainPoints;
    private double selfPointDensity;

    public SimpleKDE() {
        bwSelector = new BandwidthSelector();
    }

    public SimpleKDE setBandwidth(double[] bw) {this.bandwidth = bw; return this;}
    public SimpleKDE setKernel(Kernel k) {this.kernel = k; return this;}
    public SimpleKDE setIgnoreSelf(boolean ignoreSelf) {
        this.ignoreSelf = ignoreSelf;
        return this;
    }

    public double[] getBandwidth() {
        return bandwidth;
    }

    public SimpleKDE train(List<double[]> data) {
        this.trainPoints = data;
        // Only calculate bandwidth if it hasn't been set by user
        if (bandwidth == null) {
            bandwidth = bwSelector.findBandwidth(data);
        }
        if (kernel == null) {
            kernel = new GaussianKernel();
        }
        kernel.initialize(bandwidth);
        this.selfPointDensity = kernel.density(new double[bandwidth.length]);

        return this;
    }

    private double rawDensity(double[] d) {
        double score = 0.0;
        for (double[] v : trainPoints) {
            double[] testVec = d.clone();
            for (int i = 0; i < testVec.length; i++) {
                testVec[i] -= v[i];
            }
            double delta = kernel.density(testVec);
            score += delta;
        }
        return score;
    }

    public double density(double[] d) {
        if (ignoreSelf) {
            return (rawDensity(d) - selfPointDensity) / (trainPoints.size() - 1);
        } else {
            return rawDensity(d) / trainPoints.size();
        }
    }
}

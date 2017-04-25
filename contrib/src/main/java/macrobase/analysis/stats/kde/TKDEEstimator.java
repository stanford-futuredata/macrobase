package macrobase.analysis.stats.kde;

import macrobase.analysis.stats.kde.grid.CompositeGrid;
import macrobase.analysis.stats.kde.kernel.BandwidthSelector;
import macrobase.analysis.stats.kde.kernel.Kernel;
import macrobase.analysis.stats.kde.kernel.KernelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TKDEEstimator {
    private static final Logger log = LoggerFactory.getLogger(TKDEEstimator.class);

    public TKDEConf tConf;

    public double[] bandwidth;
    public double cutoffH, cutoffL, tolerance;
    public KDTree tree;
    public TreeKDE kde;
    public CompositeGrid grids;

    public TKDEEstimator(TKDEConf conf) {
        this.tConf = conf;
    }

    public TKDEEstimator train(List<double[]> data) {
        bandwidth = new BandwidthSelector()
                .setMultiplier(tConf.bwMultiplier)
                .findBandwidth(data);
        Kernel kernel = new KernelFactory(tConf.kernel).get().initialize(bandwidth);

        if (tConf.calculateCutoffs) {
            QuantileBoundEstimator q = new QuantileBoundEstimator(tConf);
            q.estimateQuantiles(data);
            cutoffH = q.cutoffH;
            cutoffL = q.cutoffL;
            tolerance = q.tolerance;
            tree = q.tree;
        } else {
            cutoffH = tConf.cutoffHAbsolute;
            cutoffL = tConf.cutoffLAbsolute;
            tolerance = tConf.tolAbsolute;
        }

        if (tree == null) {
            tree = new KDTree()
                    .setSplitByWidth(tConf.splitByWidth)
                    .setLeafCapacity(tConf.leafSize)
                    .build(data);
        }

        kde = new TreeKDE(tree)
                .setBandwidth(bandwidth)
                .setKernel(kernel)
                .setTrainedTree(tree)
                .setIgnoreSelf(tConf.ignoreSelfScoring)
                .setCutoffH(cutoffH)
                .setCutoffL(cutoffL)
                .setTolerance(tolerance)
                .train(data);

        if (tConf.useGrid) {
            grids = new CompositeGrid(
                    kernel,
                    bandwidth,
                    tConf.gridSizes,
                    cutoffH)
                    .setIgnoreSelf(tConf.ignoreSelfScoring)
                    .train(data);
        }
        return this;
    }

    public double density(double[] d) {
        if (tConf.useGrid) {
            double gridDensity = grids.density(d);
            if (gridDensity > 0) {
                return gridDensity;
            }
        }
        return kde.density(d);
    }

    public int getNumKernels() {
        return kde.getNumKernels();
    }
}

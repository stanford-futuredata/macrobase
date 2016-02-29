package macrobase.analysis.outlier;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.datamodel.KDTree;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TreeKDE extends KDE {

    private static final Logger log = LoggerFactory.getLogger(BinnedKDE.class);
    private KDTree kdtree;
    private int kdtreeLeafCapacity;
    private double scoreInvertingFactor;
    private double onePointTolerance;
    private final double accuracy = 1e-6;

    public TreeKDE(MacroBaseConf conf) {
        super(conf);
        kdtreeLeafCapacity = conf.getInt(MacroBaseConf.KDTREE_LEAF_CAPACITY, MacroBaseDefaults.KDTREE_LEAF_CAPACITY);
    }

    @Override
    public void train(List<Datum> data) {
        this.setBandwidth(data);
        log.debug("training BinnedKDE");
        this.kdtree = new KDTree(data, kdtreeLeafCapacity);
        this.scoreScalingFactor = 1.0 / (bandwidthDeterminantSqrt * data.size());
        this.scoreInvertingFactor = 1.0 / scoreScalingFactor;

        this.onePointTolerance = bandwidthDeterminantSqrt * accuracy;
        log.debug("onePointTolerence = {}", onePointTolerance);
    }

    private double scoreKDTree(KDTree tree, Datum datum) {
        List<RealVector> minMaxD = tree.getMinMaxDistanceVectors(datum);
        double wMin = this.scaledKernelDensity(minMaxD.get(0));
        double wMax = this.scaledKernelDensity(minMaxD.get(1));
        if (wMin - wMax < this.onePointTolerance) {
            // Return the average of the scores
            return 0.5 * (wMin + wMax) * tree.getnBelow();
        } else {
            if (tree.isLeaf()) {
                // Note: This might not be a very good approximation.. Need some performance insight.
                return tree.getnBelow() * this.scaledKernelDensity(tree.getMean());
            } else {
                return scoreKDTree(tree.getHiChild(), datum) + scoreKDTree(tree.getLoChild(), datum);
            }
        }
    }

    @Override
    public double score(Datum datum) {
        double unscaledScore = scoreKDTree(kdtree, datum);
        // Note: return score with a minus sign, s.t. outliers are selected not inliers.
        return -unscaledScore * scoreScalingFactor;
    }
}

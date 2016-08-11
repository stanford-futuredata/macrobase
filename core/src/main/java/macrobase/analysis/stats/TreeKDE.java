package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.datamodel.KDTree;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TreeKDE extends KDE {

    private static final Logger log = LoggerFactory.getLogger(TreeKDE.class);
    private KDTree kdtree;
    private int kdtreeLeafCapacity;
    private double scoreScaleLog;
    private double onePointTolerance;
    private final double accuracy;
    // Leave this off until we have a more refined appromixation, saw very bad results with true
    private boolean approximateLeaves = false;

    private int numScored = 0;

    public TreeKDE(MacroBaseConf conf) {
        super(conf);
        kdtreeLeafCapacity = conf.getInt(MacroBaseConf.KDTREE_LEAF_CAPACITY, MacroBaseDefaults.KDTREE_LEAF_CAPACITY);
        accuracy = conf.getDouble(MacroBaseConf.TREE_KDE_ACCURACY, MacroBaseDefaults.TREE_KDE_ACCURACY);
        proportionOfDataToUse = 1.0;
    }

    public void setApproximateLeaves(boolean approximateLeaves) {
        this.approximateLeaves = approximateLeaves;
    }

    @Override
    public void train(List<Datum> data) {
        this.setBandwidth(data);
        log.debug("training kd-tree KDE on {} points", data.size());
        this.kdtree = new KDTree(new ArrayList<>(data), kdtreeLeafCapacity);
        this.scoreScalingFactor = 1.0 / (bandwidthDeterminantSqrt * data.size());
        this.scoreScaleLog = Math.log(scoreScalingFactor);

        // Instead of scaling scores we scale acceptance
        this.onePointTolerance = bandwidthDeterminantSqrt * accuracy;
        log.info("using accuray = {}", accuracy);
        log.debug("onePointTolerance = {}", onePointTolerance);
    }

    private double scoreKDTree(KDTree tree, Datum datum) {
        RealVector[] minMaxD = tree.getMinMaxDistanceVectors(datum);
        double wMin = this.scaledKernelDensity(minMaxD[0]);
        double wMax = this.scaledKernelDensity(minMaxD[1]);
        if (wMin - wMax < accuracy) {
            // Return the average of the scores
            return 0.5 * (wMin + wMax) * tree.getnBelow();
        } else {
            if (tree.isLeaf()) {
                if (approximateLeaves) {
                    return tree.getnBelow() * this.scaledKernelDensity(tree.getMean());
                } else {
                    double _score = 0.0;
                    for (Datum child : tree.getItems()) {
                        RealVector difference = datum.getMetrics().subtract(child.getMetrics());
                        double _diff = this.scaledKernelDensity(difference);
                        _score += _diff;
                    }
                    return _score;
                }

            } else {
                return scoreKDTree(tree.getHiChild(), datum) + scoreKDTree(tree.getLoChild(), datum);
            }
        }
    }

    public KDTree getKdtree() {
        return kdtree;
    }

    @Override
    /**
     * Return the negative log pdf density, this avoids underflow errors while still being
     * an interpretable quantity. Use scoreDensity if you need the actual negative pdf.
     */
    public double score(Datum datum) {
        numScored++;
        if (numScored % 10000 == 0) {
            log.debug("Scored {}", numScored);
        }
        double unscaledScore = scoreKDTree(kdtree, datum);
        // Note: return score with a minus sign, s.t. outliers are selected not inliers.
        return -(Math.log(unscaledScore) + scoreScaleLog);
    }

    public double scoreDensity(Datum datum) {
        return -Math.exp(-score(datum));
    }
}

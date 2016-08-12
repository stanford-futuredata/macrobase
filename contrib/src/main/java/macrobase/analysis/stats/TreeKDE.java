package macrobase.analysis.stats;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.analysis.index.KDTree;
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

    public static final String KDTREE_LEAF_CAPACITY = "macrobase.analysis.treeKde.leafCapacity";
    public static final String TREE_KDE_ACCURACY = "macrobase.analysis.treeKde.accuracy";

    public static final Integer KDTREE_LEAF_CAPACITY_DEFAULT = 2;
    public static final Double TREE_KDE_ACCURACY_DEFAULT = 1e-5;

    public TreeKDE(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
        kdtreeLeafCapacity = conf.getInt(KDTREE_LEAF_CAPACITY, KDTREE_LEAF_CAPACITY_DEFAULT);
        accuracy = conf.getDouble(TREE_KDE_ACCURACY, TREE_KDE_ACCURACY_DEFAULT);
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
        log.info("using accuracy = {}", accuracy);
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
                        RealVector difference = datum.metrics().subtract(child.metrics());
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

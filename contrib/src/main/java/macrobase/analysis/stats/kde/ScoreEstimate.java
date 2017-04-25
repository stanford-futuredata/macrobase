package macrobase.analysis.stats.kde;

import macrobase.analysis.stats.kde.kernel.Kernel;

/**
 * Class used to tracking the weight contributions of different regions of space.
 * Stored in a priority queue and split when we need more refined estimates.
 */
public class ScoreEstimate {
    public KDTree tree;
    protected double wMax, wMin;
    public double totalWMax, totalWMin;

    public ScoreEstimate(Kernel kernel, KDTree tree, double[] d) {
        this.tree = tree;
        double[][] minMaxD = tree.getMinMaxDistanceVectors(d);
        this.wMax = kernel.density(minMaxD[0]);
        this.wMin = kernel.density(minMaxD[1]);
        int n = tree.getNBelow();
        this.totalWMax = wMax * n;
        this.totalWMin = wMin * n;
    }

    public ScoreEstimate[] split(Kernel kernel, double[] d) {
        ScoreEstimate[] children = new ScoreEstimate[2];
        children[0] = new ScoreEstimate(kernel, this.tree.getLoChild(), d);
        children[1] = new ScoreEstimate(kernel, this.tree.getHiChild(), d);
        return children;
    }

    @Override
    public String toString() {
        return String.format(
                "[%f, %f]:%d:(%f, %f)",
                totalWMin,
                totalWMax,
                tree.getNBelow(),
                wMin,
                wMax
                );
    }
}

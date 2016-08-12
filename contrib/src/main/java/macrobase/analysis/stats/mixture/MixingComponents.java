package macrobase.analysis.stats.mixture;

public interface MixingComponents {
    double[] calcExpectationLog();
    void update(double[][] r);

    void moveNatural(double[][] r, double pace, double v);

    double[] getNormalizedClusterProportions();
}

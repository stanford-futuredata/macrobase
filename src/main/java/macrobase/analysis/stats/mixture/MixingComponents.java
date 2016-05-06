package macrobase.analysis.stats.mixture;

public interface MixingComponents {
    double[] calcExpectationLog();
    void update(double[][] r);

}

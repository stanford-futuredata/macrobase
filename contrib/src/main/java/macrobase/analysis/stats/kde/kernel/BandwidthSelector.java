package macrobase.analysis.stats.kde.kernel;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.List;

/**
 * Use Scott's rule to estimate a default bandwidth for multivariate data
 */
public class BandwidthSelector {
    private double userMultiplier = 1.0;

    public BandwidthSelector setMultiplier(double m) {this.userMultiplier = m; return this;}

    public double[] findBandwidth(List<double[]> input) {
        int dim = input.get(0).length;
        int n = input.size();
        double scaleFactor = Math.pow(n, -1.0/(dim+4));

        Percentile pCalc = new Percentile();
        double[] bandwidth = new double[dim];

        for (int d = 0; d < dim; d++) {
            double[] xs = new double[n];
            for (int i = 0; i < n; i++) {
                xs[i] = input.get(i)[d];
            }
            pCalc.setData(xs);
            bandwidth[d] = (pCalc.evaluate(75) - pCalc.evaluate(25)) * scaleFactor * userMultiplier;
        }

        return bandwidth;
    }
}

package macrobase.analysis.stats;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Winsorizer {
    public double trimPct;

    public double[][] bounds;

    public Winsorizer(double trimPct) {
        this.trimPct = trimPct;
    }

    public List<double[]> process(List<double[]> metrics) {
        int n = metrics.size();
        int k = metrics.get(0).length;
        Percentile p = new Percentile();
        bounds = new double[k][2];
        List<double[]> newMetrics = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            newMetrics.add(new double[k]);
        }

        double[] curDimensionValues = new double[n];
        for (int j = 0; j < k; j++) {
            for (int i = 0; i < n; i++) {
                curDimensionValues[i] = metrics.get(i)[j];
            }
            p.setData(curDimensionValues);
            bounds[j][0] = p.evaluate(trimPct);
            bounds[j][1] = p.evaluate(100 - trimPct);
            for (int i = 0; i < n; i++) {
                double curValue = curDimensionValues[i];
                if (curValue > bounds[j][1]) {
                    newMetrics.get(i)[j] = bounds[j][1];
                } else if (curValue < bounds[j][0]) {
                    newMetrics.get(i)[j] = bounds[j][0];
                } else {
                    newMetrics.get(i)[j] = curValue;
                }
            }
        }

        return newMetrics;
    }
}

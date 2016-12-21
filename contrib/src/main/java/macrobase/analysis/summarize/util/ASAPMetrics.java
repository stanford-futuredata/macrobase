package macrobase.analysis.summarize.util;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Variance;

import java.util.List;

public class ASAPMetrics {
    public Kurtosis kurtosis;
    public Variance variance;
    public double originalKurtosis;
    public int metricIdx = 1;

    public ASAPMetrics() {
        kurtosis = new Kurtosis();
        variance = new Variance();
    }

    public ASAPMetrics(List<Datum> data, int metricIdx) {
        this();
        this.metricIdx = metricIdx;
        originalKurtosis = kurtosis(data);
    }

    private double[] diffs(double[] values) {
        double[] slopes = new double[values.length - 1];
        for (int i = 1; i < values.length; i++) {
            slopes[i - 1] = values[i] - values[i - 1];
        }
        return slopes;
    }

    private double[] stripDatum(List<Datum> data) {
        assert(data.get(0).metrics().getDimension() == 2);
        double[] values = new double[data.size()];
        for (int i = 0; i < data.size(); i ++) {
            values[i] = data.get(i).metrics().getEntry(metricIdx);
        }
        return values;
    }

    public void updateKurtosis(List<Datum> data) {
        originalKurtosis = kurtosis(data);
    }

    public double roughness(List<Datum> data) {
        double[] slopes = diffs(stripDatum(data));
        return Math.sqrt(variance.evaluate(slopes));
    }

    public double kurtosis(List<Datum> data) {
        return kurtosis.evaluate(stripDatum(data));
    }

    public double[] roughnessKurtosis(List<Datum> data) {
        double[] values = stripDatum(data);
        double[] results = new double[2];
        double[] slopes = diffs(values);
        results[0] = Math.sqrt(variance.evaluate(slopes));
        results[1] = kurtosis.evaluate(values);
        return results;
    }
}

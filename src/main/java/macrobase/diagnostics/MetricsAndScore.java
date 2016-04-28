package macrobase.diagnostics;

import org.apache.commons.math3.linear.RealVector;

public class MetricsAndScore {
    private RealVector metrics;
    private double score;

    public MetricsAndScore(RealVector metrics, double score) {
        this.metrics = metrics;
        this.score = score;
    }

    public RealVector getMetrics() {
        return metrics;
    }

    public double getScore() {
        return score;
    }
}

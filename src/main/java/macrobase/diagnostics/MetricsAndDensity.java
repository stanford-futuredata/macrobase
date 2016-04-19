package macrobase.diagnostics;

import org.apache.commons.math3.linear.RealVector;

public class MetricsAndDensity {
    private RealVector metrics;
    private double density;

    public MetricsAndDensity(RealVector metrics, double density) {
        this.metrics = metrics;
        this.density = density;
    }

    public RealVector getMetrics() {
        return metrics;
    }

    public double getDensity() {
        return density;
    }
}

package macrobase.diagnostics;

import org.apache.commons.math3.linear.RealVector;

public class MetricsAndMetrics {
    RealVector metrics;
    RealVector transformedMetrics;

    public MetricsAndMetrics(RealVector beforeMetrics, RealVector afterMetrics) {
        metrics = beforeMetrics;
        transformedMetrics = afterMetrics;
    }
}

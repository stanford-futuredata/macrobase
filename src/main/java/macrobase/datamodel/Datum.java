package macrobase.datamodel;

import org.apache.commons.math3.linear.RealVector;

import java.util.List;

public class Datum implements HasMetrics {
    private List<Integer> attributes;
    private RealVector metrics;

    public Datum(List<Integer> attributes, RealVector metrics) {
        this.attributes = attributes;
        this.metrics = metrics;
    }

    public List<Integer> getAttributes() {
        return attributes;
    }

    @Override
    public RealVector getMetrics() {
        return metrics;
    }
}

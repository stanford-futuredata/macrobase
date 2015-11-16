package macrobase.datamodel;

import java.util.List;

public class Datum {
    private List<Integer> attributes;
    private List<Double> metrics;

    public Datum(List<Integer> attributes, List<Double> metrics) {
        this.attributes = attributes;
        this.metrics = metrics;
    }

    public List<Integer> getAttributes() {
        return attributes;
    }

    public List<Double> getMetrics() {
        return metrics;
    }
}

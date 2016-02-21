package macrobase.datamodel;

import org.apache.commons.math3.linear.RealVector;

import java.util.List;

public class Datum implements HasMetrics {
    private List<Integer> attributes;
    private RealVector metrics;
    private RealVector auxiliaries;

    public Datum() {
    }

    public Datum(List<Integer> attributes, RealVector metrics) {
        this.attributes = attributes;
        this.metrics = metrics;
    }

    public Integer getTime() {
        return null;
    }

    public List<Integer> getAttributes() {
        return attributes;
    }

    @Override
    public RealVector getMetrics() {
        return metrics;
    }

    public RealVector getAuxiliaries() {
        return this.auxiliaries;
    }

    public void setAuxiliaries(RealVector auxiliaries) {
        this.auxiliaries = auxiliaries;
    }

    public String toString() {
        return String.format(
                "metrics: %s, encoded attributes: %s, auxiliaries: %s",
                getMetrics().toString(), getAttributes().toString(),
                String.valueOf(getAuxiliaries()));
    }
}

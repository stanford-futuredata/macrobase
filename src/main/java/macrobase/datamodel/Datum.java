package macrobase.datamodel;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
}

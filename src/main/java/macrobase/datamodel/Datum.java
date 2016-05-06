package macrobase.datamodel;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Datum implements HasMetrics {
    private static AtomicLong idGen = new AtomicLong();

    private final Long id;
    private List<Integer> attributes;
    private RealVector metrics;
    private RealVector auxiliaries;



    private List<Integer> contextualDiscreteAttributes;
    private RealVector contextualDoubleAttributes;

    private final Long parentDatumID; //the parent datum this datum is created from

    public Datum() {
        id = idGen.incrementAndGet();
        parentDatumID = null;
    }

    public Datum(Datum oldDatum, double... doubleMetrics) {
        this(oldDatum, new ArrayRealVector(doubleMetrics));
    }

    public Datum(Datum oldDatum, RealVector metrics) {
        this.id = idGen.incrementAndGet();
        this.parentDatumID = oldDatum.getID();
        this.metrics = metrics;
        this.attributes = oldDatum.getAttributes();
        this.contextualDiscreteAttributes = oldDatum.getContextualDiscreteAttributes();
        this.contextualDoubleAttributes = oldDatum.getContextualDoubleAttributes();
        this.auxiliaries = oldDatum.getAuxiliaries();
    }

    public Datum(List<Integer> attributes, double... doubleMetrics) {
        this(attributes, new ArrayRealVector(doubleMetrics));
    }

    public Datum(List<Integer> attributes, RealVector metrics) {
        this();
        this.attributes = attributes;
        this.metrics = metrics;
    }

    public Datum(List<Integer> attributes, RealVector metrics, List<Integer> contextualDiscreteAttributes, RealVector contextualDoubleAttributes) {
        this();
        this.attributes = attributes;
        this.metrics = metrics;
        this.contextualDiscreteAttributes = contextualDiscreteAttributes;
        this.contextualDoubleAttributes = contextualDoubleAttributes;
    }

    public int getTime(Integer timeColumn) {
        return (int) metrics.getEntry(timeColumn);
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

    public Long getID() {
        return id;
    }

    public Long getParentID() {
        return parentDatumID;
    }

    public String toString() {
        return String.format(
                "metrics: %s, encoded attributes: %s, auxiliaries: %s",
                getMetrics().toString(), getAttributes().toString(),
                String.valueOf(getAuxiliaries()));
    }

    public List<Integer> getContextualDiscreteAttributes() {
        if (contextualDiscreteAttributes != null) {
            return contextualDiscreteAttributes;
        } else {
            return new ArrayList<>();
        }
    }

    public RealVector getContextualDoubleAttributes() {
        if (contextualDoubleAttributes != null) {
            return contextualDoubleAttributes;
        } else {
            return new ArrayRealVector(0);
        }
    }

}

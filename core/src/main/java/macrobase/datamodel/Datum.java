package macrobase.datamodel;

import com.google.common.collect.Lists;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Datum {
    private static AtomicLong idGen = new AtomicLong();

    private final Long id;
    private List<Integer> attributes;
    private RealVector metrics;

    private final Long parentDatumID; //the parent datum this datum is created from

    public Datum() {
        id = idGen.incrementAndGet();
        parentDatumID = null;
    }

    public Datum(Datum oldDatum) {
        this();
        this.attributes = Lists.newArrayList(oldDatum.attributes);
        this.metrics = oldDatum.metrics().copy();
    }

    public Datum(Datum oldDatum, double... doubleMetrics) {
        this(oldDatum, new ArrayRealVector(doubleMetrics));
    }

    public Datum(Datum oldDatum, RealVector metrics) {
        this.id = idGen.incrementAndGet();
        this.parentDatumID = oldDatum.getID();
        this.metrics = metrics;
        this.attributes = oldDatum.attributes();
    }

    public Datum(List<Integer> attributes, double... doubleMetrics) {
        this(attributes, new ArrayRealVector(doubleMetrics));
    }

    public Datum(List<Integer> attributes, RealVector metrics) {
        this();
        this.attributes = attributes;
        this.metrics = metrics;
    }
    
    public long getTime(Integer timeColumn) {
        return (long) metrics.getEntry(timeColumn);
    }

    public List<Integer> attributes() {
        return attributes;
    }

    public RealVector metrics() {
        return metrics;
    }

    public Long getID() {
        return id;
    }

    public Long getParentID() {
        return parentDatumID;
    }

    public String toString() {
        return String.format(
                "metrics: %s, encoded attributes: %s",
                metrics().toString(), attributes().toString());
    }

    public double norm() {
        return metrics().getNorm();
    }
}

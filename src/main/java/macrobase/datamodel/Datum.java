package macrobase.datamodel;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Datum implements HasMetrics {
    private List<Integer> attributes;
    private RealVector metrics;

    public Datum() {}

    public Datum(List<Integer> attributes, RealVector metrics) {
        this.attributes = attributes;
        this.metrics = metrics;
    }

    public Datum(Datum other,
                 Set<Integer> attributesToKeep,
                 Set<Integer> metricDimensionsToKeep) {
        this.attributes = new ArrayList<>();

        for(int i = 0; i < other.getAttributes().size(); ++i) {
            if(attributesToKeep.contains(i)) {
                attributes.add(other.getAttributes().get(i));
            }
        }

        metrics = new ArrayRealVector(metricDimensionsToKeep.size());

        int curIdx = 0;
        for(int i = 0; i < other.metrics.getDimension(); ++i) {
            if(metricDimensionsToKeep.contains(i)) {
                metrics.setEntry(curIdx, other.getMetrics().getEntry(i));
                curIdx++;
            }
        }
    }

    public List<Integer> getAttributes() {
        return attributes;
    }

    @Override
    public RealVector getMetrics() {
        return metrics;
    }
}

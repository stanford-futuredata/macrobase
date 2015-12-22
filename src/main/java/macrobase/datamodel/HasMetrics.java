package macrobase.datamodel;

import org.apache.commons.math3.linear.RealVector;

// kind of a hack
public interface HasMetrics {
    RealVector getMetrics();
}

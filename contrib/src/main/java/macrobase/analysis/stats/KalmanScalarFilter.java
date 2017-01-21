package macrobase.analysis.stats;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper around KalmanVectorFilter, allows passing scalars to the Kalman Filter and receive scalars instead of vectors.
 */
public class KalmanScalarFilter extends KalmanVectorFilter {
    private static final Logger log = LoggerFactory.getLogger(KalmanScalarFilter.class);

    private static RealVector toVector(double x) {
        RealVector v = new ArrayRealVector(1);
        v.setEntry(0, x);
        return v;
    }

    public KalmanScalarFilter(double startLoc, double qScale) {
        super(toVector(startLoc), qScale);
    }

    public double step(double observation, int time) {
        RealVector v = super.step(toVector(observation), time);
        return v.getEntry(0);
    }
}

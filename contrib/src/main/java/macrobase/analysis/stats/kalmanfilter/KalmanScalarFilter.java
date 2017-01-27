package macrobase.analysis.stats.kalmanfilter;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper around KalmanVectorFilter that works on doubles instead of vectors,
 * i.e. constructor accepts a double instead of a vector and step method operates on a double and returns a double.
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

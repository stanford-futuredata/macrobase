package macrobase.analysis.stats;

import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    public void reset(double startLoc) {
        super.reset(toVector(startLoc));
    }

    public double step(double observation, int time) {
        RealVector v = super.step(toVector(observation), time);
        return v.getEntry(0);
    }
}

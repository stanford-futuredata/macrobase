package macrobase.analysis.stats;

import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper around KalmanVectorFilter, allows passing matrices to the Kalman Filter and receive matrices instead of vectors.
 */
public class KalmanFlattenedMatrixFilter extends KalmanVectorFilter {
    private static final Logger log = LoggerFactory.getLogger(KalmanFlattenedMatrixFilter.class);

    public KalmanFlattenedMatrixFilter(RealMatrix startLoc, double qScale, double rScale) {
        super(AlgebraUtils.flattenMatrixByColumns(startLoc), qScale, rScale);
    }

    public RealMatrix step(RealMatrix observation, int time) {
        RealVector v = super.step(AlgebraUtils.flattenMatrixByColumns(observation), time);
        return AlgebraUtils.reshapeMatrixByColumns(v, observation);
    }
}

package macrobase.analysis.stats.kalmanfilter;

import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper around KalmanVectorFilter that works on matrices instead of vectors,
 * i.e. constructor accepts a matrix instead of a vector and step method operates on a matrix and returns a matrix.
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

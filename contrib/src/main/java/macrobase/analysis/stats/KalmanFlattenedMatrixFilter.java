package macrobase.analysis.stats;

import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KalmanFlattenedMatrixFilter extends KalmanVectorFilter {
    private static final Logger log = LoggerFactory.getLogger(KalmanFlattenedMatrixFilter.class);

    public KalmanFlattenedMatrixFilter(RealMatrix startLoc, double qScale) {
        super(AlgebraUtils.flattenMatrixByColumns(startLoc), qScale);
    }

    public RealMatrix step(RealMatrix observation, int time) {
        RealVector v = super.step(AlgebraUtils.flattenMatrixByColumns(observation), time);
        return AlgebraUtils.reshapeMatrixByColumns(v, observation);
    }

    public void reset(RealMatrix observation) {
        RealVector v = AlgebraUtils.flattenMatrixByColumns(observation);
        super.reset(v);
    }
}

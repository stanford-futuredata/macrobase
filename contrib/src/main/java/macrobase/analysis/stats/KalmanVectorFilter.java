package macrobase.analysis.stats;

import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KalmanVectorFilter {
    private static final Logger log = LoggerFactory.getLogger(KalmanVectorFilter.class);
    protected double qScale;
    private final RealMatrix H; // measurement operator
    protected RealMatrix state;
    protected RealMatrix cov;

    // TODO: Deprecate
    public KalmanVectorFilter(int D) {
        this(D, 1e-6);
    }

    public KalmanVectorFilter(int D, double qScale) {
        this.qScale = qScale;
        double[][] tmp = {{1, 0}};
        H = new BlockRealMatrix(tmp);
        // 1 row for location, 1 row for velocity.
        state = new BlockRealMatrix(2, D);
        cov = MatrixUtils.createRealIdentityMatrix(2);
    }


    public KalmanVectorFilter(RealVector startLoc, double qScale) {
        this.qScale = qScale;
        double[][] tmp = {{1, 0}};
        H = new BlockRealMatrix(tmp);
        reset(startLoc);
    }


    public void reset(RealVector startLoc) {
        int D = startLoc.getDimension();
        state = new BlockRealMatrix(2, D);
        state.setRowVector(0, startLoc);
        cov = MatrixUtils.createRealIdentityMatrix(2);
    }

    protected RealVector measure(RealMatrix state) {
        return state.getRowVector(0);
    }

    public RealVector step(RealVector observation, int time) {
        RealVector g = new ArrayRealVector(2);
        g.setEntry(0, 0.5 * time * time);
        g.setEntry(1, time);
        RealMatrix Q = g.outerProduct(g).scalarMultiply(qScale);
        RealMatrix R = MatrixUtils.createRealIdentityMatrix(1);
        R = R.scalarMultiply(time * time);

        RealMatrix F = MatrixUtils.createRealIdentityMatrix(2);
        F.setEntry(0, 1, time);

        RealMatrix priorNextState = F.multiply(state);
        RealMatrix priorNextCov = F.multiply(cov).multiply(F.transpose()).add(Q); // F * cov * F^T + Q

        RealVector measurementResidual = observation.subtract(measure(priorNextState)); // row vector
        RealMatrix residualCovariance = H.multiply(priorNextCov).multiply(H.transpose()).add(R);
        RealMatrix kalmanGain = priorNextCov.multiply(H.transpose()).multiply(AlgebraUtils.invertMatrix(residualCovariance));
        // kalmanGain should be a 1x2 matrix
        state = priorNextState.add(kalmanGain.getColumnVector(0).outerProduct(measurementResidual));
        cov = MatrixUtils.createRealIdentityMatrix(2).subtract(kalmanGain.multiply(H)).multiply(priorNextCov);
        return measure(state);
    }
}

package macrobase.analysis.stats.kalmanfilter;

import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kalman Filter for a vector without user input at every step (u = 0) https://en.wikipedia.org/wiki/Kalman_filter#Details
 * Currently user has to specify measurement and noise estimates using qScale (process noise) and rScale (measurement noise)
 */
public class KalmanVectorFilter {
    private static final Logger log = LoggerFactory.getLogger(KalmanVectorFilter.class);
    protected double qScale;
    protected double rScale;
    private final RealMatrix H; // measurement operator
    protected RealMatrix state;
    protected RealMatrix cov;

    public KalmanVectorFilter(RealVector startLoc, double qScale) {
        this(startLoc, qScale, 1);
    }

    /**
     * @param startLoc location at time 0
     * @param qScale process noise variance per unit time
     * @param rScale measurement noise variance
     */
    public KalmanVectorFilter(RealVector startLoc, double qScale, double rScale) {
        this.qScale = qScale;
        this.rScale = rScale;
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

    /**
     * Implement a filtering step, for more information see:
     * https://en.wikipedia.org/wiki/Kalman_filter#Example_application.2C_technical
     * @param observation observed value for the vector
     * @param time time interval from previous observation
     * @return filtered vector
     */
    public RealVector step(RealVector observation, int time) {
        // Create g = [t^2 / 2, t]^T matrix
        RealVector g = new ArrayRealVector(2);
        g.setEntry(0, 0.5 * time * time);
        g.setEntry(1, time);
        RealMatrix Q = g.outerProduct(g).scalarMultiply(qScale);
        RealMatrix R = MatrixUtils.createRealIdentityMatrix(1).scalarMultiply(rScale);
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

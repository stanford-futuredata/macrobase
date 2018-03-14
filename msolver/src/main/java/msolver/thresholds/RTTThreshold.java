package msolver.thresholds;

import msolver.util.MathUtil;
import msolver.SimpleBoundSolver;
import msolver.struct.MomentStruct;

public class RTTThreshold implements MomentThreshold {
    private MomentStruct ms;

    public RTTThreshold(MomentStruct ms) {
        this.ms = ms;
    }

    public double[] bound(double x) {
        double[] xs = new double[]{x};
        double[] gttBounds = new double[]{0.0, 1.0};
        double[] moments;
        SimpleBoundSolver boundSolver;
        double[] boundSizes;

        int ka = ms.powerSums.length;
        int kb = ms.logSums.length;

        // Standard basis
        moments = MathUtil.powerSumsToMoments(ms.powerSums);
        boundSolver = new SimpleBoundSolver(ka);
        try {
            boundSizes = boundSolver.solveBounds(moments, xs);
            double[] standardBounds = boundSolver.getBoundEndpoints(moments, x, boundSizes[0]);
            if (1.0 - standardBounds[1] > gttBounds[0]) {
                gttBounds[0] = 1.0 - standardBounds[1];
            }
            if (1.0 - standardBounds[0] < gttBounds[1]) {
                gttBounds[1] = 1.0 - standardBounds[0];
            }
        } catch (Exception e) {}

        // Log basis
        double[] logXs = new double[]{Math.log(x)};
        moments = MathUtil.powerSumsToMoments(ms.logSums);
        try {
            boundSolver = new SimpleBoundSolver(kb);
            boundSizes = boundSolver.solveBounds(moments, logXs);
            double[] logBounds = boundSolver.getBoundEndpoints(moments, Math.log(x), boundSizes[0]);
            if (1.0 - logBounds[1] > gttBounds[0]) {
                gttBounds[0] = 1.0 - logBounds[1];
            }
            if (1.0 - logBounds[0] < gttBounds[1]) {
                gttBounds[1] = 1.0 - logBounds[0];
            }
        } catch (Exception e) {}

        return gttBounds;
    }
}

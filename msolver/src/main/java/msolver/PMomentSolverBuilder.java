package msolver;

import msolver.struct.ArcSinhMomentStruct;

import java.util.Arrays;

/**
 * Public interface to the moment solver
 */
public class PMomentSolverBuilder {
    private boolean verbose;
    private ArcSinhMomentStruct ms;
    private PointMassSolver solver;

    private int callType;

    public PMomentSolverBuilder(ArcSinhMomentStruct ms) {
        this.ms = ms;
    }
    public void setVerbose(boolean flag) {
        this.verbose = flag;
    }

    public void initialize() {
    }
    public double getCDF(double x) {
        solve();
        return solver.getCDF(ms.convert(x));
    }
    public double getQuantile(double p) {
        solve();
        return ms.invert(solver.getQuantile(p));
    }
    public boolean checkThreshold(double xRaw, double phi) {
        int k = ms.powerSums.length;
        double x = ms.convert(xRaw);
        if (x < -1) {
            callType = 0;
            return (phi <= 1);
        }
        if (x >= 1) {
            callType = 0;
            return (phi <= 0);
        }
        if (phi > 1) {
            callType = 0;
            return false;
        }
        if (x > 0) {
            double[] powerMoments = ms.getPowerMoments();
            for (int i = 2; i < k; i+=2) {
                double mLowerBound = Math.pow(x, i) * phi;
                if (mLowerBound > powerMoments[i]) {
                    callType = 1;
                    return false;
                }
            }
        }

        long startTime = System.nanoTime();
        solve();
        callType = 2;
        double cdfValue = solver.getCDF(x);
        long endTime = System.nanoTime();
        double elapsed = (endTime - startTime) * 1.0e-6;
        if (verbose) {
            if (elapsed > 1) {
                System.out.println("long: " + elapsed);
                System.out.println(ms.toString());
            }
        }
        boolean retVal = 1 - cdfValue >= phi;
        return retVal;
    }

    private void solve() {
        int k = ms.powerSums.length;
        if (solver == null) {
            solver = new PointMassSolver(k);
            solver.setVerbose(verbose);
            solver.solve(ms.getPowerMoments());
        } else {
            return;
        }
    }

    public int getCallType() {
        return callType;
    }
}

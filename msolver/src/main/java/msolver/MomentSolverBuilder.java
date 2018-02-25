package msolver;

import msolver.struct.MomentStruct;
import msolver.thresholds.MarkovThreshold;
import msolver.thresholds.MomentThreshold;
import msolver.thresholds.RTTThreshold;

import java.util.Arrays;

/**
 * Public interface to the moment solver
 */
public class MomentSolverBuilder {
    private boolean verbose;
    private MomentStruct ms;

    private ChebyshevMomentSolver2 solver;
    private MomentThreshold[] cascade;

    public MomentSolverBuilder(MomentStruct ms) {
        this.ms = ms;
    }
    public void setVerbose(boolean flag) {
        this.verbose = flag;
    }

    public void initialize() {
        this.cascade = new MomentThreshold[2];
        this.cascade[0] = new MarkovThreshold(ms);
        this.cascade[1] = new RTTThreshold(ms);
    }
    public double getCDF(double x) {
        solve();
        return solver.estimateCDF(x);
    }
    public double[] getQuantiles(double[] ps) {
        solve();
        return solver.estimateQuantiles(ps);
    }
    public boolean checkThreshold(double x, double phi) {
        int ka = ms.powerSums.length;
        if (ka > 0) {
            if (ms.min == ms.max) {
                return x > ms.min;
            }
        } else {
            if (ms.logMin == ms.logMax) {
                return x > Math.exp(ms.logMin);
            }
        }

        if (x < ms.min) {
            return true;
        }
        if (x > ms.max) {
            return false;
        }

        for (int i = 0; i < cascade.length; i++) {
            MomentThreshold mt = cascade[i];
            double[] bounds = mt.bound(x);
            if (bounds[0] > phi) {
                if (verbose) {
                    System.out.println("Above threshold: "+i);
                }
                return true;
            }
            if (bounds[1] < phi) {
                if (verbose) {
                    System.out.println("Below threshold: " + i);
                }
                return false;
            }
        }

        solve();
        double cdfValue = solver.estimateCDF(x);
        if (cdfValue < 1 - phi) {
            return true;
        } else {
            return false;
        }
    }

    private void solve() {
        if (solver == null) {
            solver = buildSolver();
            solver.solve(1e-8);
        }
        else {
            return;
        }
    }
    public ChebyshevMomentSolver2 buildSolver() {
        double[] posPowerMoments = MathUtil.powerSumsToPosMoments(
                ms.powerSums, ms.min, ms.max
        );
        double[] posLogMoments = MathUtil.powerSumsToPosMoments(
                ms.logSums, ms.logMin, ms.logMax
        );
        double[] powerChebyMoments = MathUtil.powerSumsToChebyMoments(
                ms.min, ms.max, ms.powerSums
        );
        double[] logChebyMoments = MathUtil.powerSumsToChebyMoments(
                ms.logMin, ms.logMax, ms.logSums
        );

        // compare whether log moments or standard moments are closer to uniform distribution
        double powerDelta = MathUtil.deltaFromUniformMoments(posPowerMoments);
        double logDelta = MathUtil.deltaFromUniformMoments(posLogMoments);
        int powerSmall = MathUtil.numSmallerThan(powerChebyMoments, 1.1);
        int logSmall = MathUtil.numSmallerThan(logChebyMoments, 1.1);
        boolean useStandardBasis = true;
        if (logSmall >= powerSmall) {
            if (logDelta < powerDelta) {
                useStandardBasis = false;
            }
        }

        double[] aMoments, bMoments;
        double aMin, aMax, bMin, bMax;
        if (useStandardBasis) {
            aMoments = powerChebyMoments;
            bMoments = logChebyMoments;
            aMin = ms.min; aMax = ms.max; bMin = ms.logMin; bMax = ms.logMax;
        } else {
            aMoments = logChebyMoments;
            bMoments = powerChebyMoments;
            bMin = ms.min; bMax = ms.max; aMin = ms.logMin; aMax = ms.logMax;
        }

        double aCenter = (aMax + aMin)/2;
        double aScale = (aMax - aMin)/2;
        double bCenter = (bMax + bMin)/2;
        double bScale = (bMax - bMin)/2;

        // Don't use all of the secondary powers to solve, the acc / speed tradeoff
        // isn't worth it.
        SolveBasisSelector sel = new SolveBasisSelector();
        sel.select(useStandardBasis, aMoments, bMoments, aCenter, aScale, bCenter, bScale);
        int ka = sel.getKa();
        int kb = sel.getKb();
        aMoments = Arrays.copyOf(aMoments, ka);
        bMoments = Arrays.copyOf(bMoments, kb);

//        System.out.println(aMin+","+aMax+","+Arrays.toString(aMoments)+","+Arrays.toString(bMoments));

        double[] combinedMoments = new double[aMoments.length + bMoments.length - 1];
        for (int i = 0; i < aMoments.length; i++) {
            combinedMoments[i] = aMoments[i];
        }
        for (int i = 0; i < bMoments.length - 1; i++) {
            combinedMoments[i + aMoments.length] = bMoments[i + 1];
        }
        ChebyshevMomentSolver2 newSolver = new ChebyshevMomentSolver2(
                useStandardBasis,
                aMoments.length,
                combinedMoments,
                aCenter,
                aScale,
                bCenter,
                bScale
        );
        newSolver.setVerbose(verbose);
        return newSolver;
    }
}

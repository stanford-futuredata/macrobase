package msolver;

import msolver.struct.ArcSinhMomentStruct;
import msolver.util.MathUtil;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class BCMomentSolverTest {
    @Test
    public void testExp() throws IOException {
        double[] mus = {1.0,
                -0.8271002837032394,
                0.42809678837249526,
                -0.037665535678591558,
                -0.19538317710136943,
                0.26546895117683184,
                -0.24024835074488254,
                0.18511830516554156,
                -0.13495665414439376,
                0.098911533528841453};
        BCMomentSolver ms = new BCMomentSolver(
                64, mus.length
        );
        ms.setVerbose(false);
        long startTime = System.nanoTime();
        int numTrials = 1;
        for (int i = 0; i < numTrials; i++) {
            ms.solve(mus);
        }
        long endTime = System.nanoTime();
        long elapsed = endTime - startTime;
        double timePerTrial = elapsed*1.0e-9/numTrials;
    }

    @Test
    public void testUniform() {
        int k = 11;
        int n = 10000;
        double[] xs = new double[n];
        for (int i = 0; i < n; i++) {
            xs[i] = i;
        }
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(k);
        ms.add(xs);

        double[] mus = ms.getChebyMoments();
        BCMomentSolver solver = new BCMomentSolver(
                64, mus.length
        );
        solver.setTolerance(1e-6);
        solver.setVerbose(false);
        solver.solve(mus);

        double p50 = ms.invert(solver.getQuantile(.5));
        assertTrue(p50 > n/2 - n/50);
        assertTrue(p50 < n/2 + n/50);
    }

        @Test
    public void testHard() throws IOException {
        int n = 10000;
        double[] xs = new double[2*n];
        int idx = 0;
        for (int i = 0; i < n; i++) {
            xs[idx++] = i;
        }
        for (int i = 0; i < n; i++) {
            xs[idx++] = 0;
        }
        int k = 10;
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(k);
        ms.add(xs);
        double[] c_mus = MathUtil.powerSumsToChebyMoments(
                ms.min,
                ms.max,
                ms.powerSums
        );

        BCMomentSolver solver = new BCMomentSolver(
                64, k
        );
        solver.setMaxSteps(20);
        solver.setTolerance(1e-8);
        solver.setVerbose(false);

        long startTime = System.nanoTime();
        int numTrials = 1;
        for (int i = 0; i < numTrials; i++) {
            solver.solve(c_mus);
        }
        long endTime = System.nanoTime();
        long elapsed = endTime - startTime;
        double timePerTrial = elapsed*1.0e-9/numTrials;
//        System.out.println(timePerTrial);

        double q25 = ms.invert(solver.getQuantile(.25));
        double q75 = ms.invert(solver.getQuantile(.75));
        assertTrue(q75 > 4800);
        assertTrue(q75 < 5200);
    }
}
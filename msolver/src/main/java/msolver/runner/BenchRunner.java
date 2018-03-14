package msolver.runner;

import msolver.BCMomentSolver;
import msolver.PointMassSolver;
import msolver.struct.ArcSinhMomentStruct;
import msolver.util.MathUtil;

import java.io.IOException;

public class BenchRunner {

    public static double testHard() {
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
        double[] c_mus = ms.getChebyMoments();
        double[] p_mus = ms.getPowerMoments();

        BCMomentSolver csolver = new BCMomentSolver(
                64, k
        );
        csolver.setMaxSteps(20);
        csolver.setTolerance(1e-8);
        csolver.setVerbose(false);

        PointMassSolver psolver = new PointMassSolver(
                k
        );
        psolver.setVerbose(false);

        long startTime = System.nanoTime();
        int numTrials = 40000;
        for (int i = 0; i < numTrials; i++) {
//            csolver.solve(c_mus);
            psolver.solve(p_mus);
        }
        long endTime = System.nanoTime();
        long elapsed = endTime - startTime;
        double timePerTrial = elapsed*1.0e-9/numTrials;
        return timePerTrial;
    }

    public static double testEasy() throws Exception {
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
                512, mus.length
        );
        long startTime = System.nanoTime();
        int numTrials = 30000;
//        int numTrials = 1;
        for (int i = 0; i < numTrials; i++) {
            ms.solve(mus);
        }
        long endTime = System.nanoTime();
        long elapsed = endTime - startTime;
        double timePerTrial = elapsed*1.0e-9/numTrials;

        return timePerTrial;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Hello");
        System.in.read();
        double timePerTrial;
        timePerTrial = testHard();
        System.out.println(timePerTrial);
    }
}

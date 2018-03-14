package msolver;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class BinnedMomentSolverTest {
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
        BinnedMomentSolver ms = new BinnedMomentSolver(
                200, mus.length
        );
        long startTime = System.nanoTime();
//        int numTrials = 30000;
        int numTrials = 1;
        for (int i = 0; i < numTrials; i++) {
            ms.solve(mus, 0, 1);
        }
        long endTime = System.nanoTime();
        long elapsed = endTime - startTime;
        double timePerTrial = elapsed*1.0e-9/numTrials;
//        System.out.println(timePerTrial);
    }

}
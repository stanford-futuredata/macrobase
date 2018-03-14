package msolver;

import msolver.struct.ArcSinhMomentStruct;
import msolver.util.MathUtil;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class PointMassSolverTest {
    @Test
    public void testSinglePoint() {
        int k = 11;
        double[] xs = {100};
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(k);
        ms.add(xs);
        double[] mus = ms.getPowerMoments();
        PointMassSolver solver = new PointMassSolver(k);
        solver.setVerbose(false);
        solver.solve(mus);
        assertEquals(
                100.0,
                ms.invert(solver.getQuantile(.6)),
                1e-7
        );
        assertEquals(
                0.0,
                solver.getCDF(ms.convert(90)),
                1e-7
        );
    }


    @Test
    public void testSmallDiscrete() {
        int k = 11;
        double[] xs = {100, 200, 300};
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(k);
        ms.add(xs);
        double[] mus = ms.getPowerMoments();
        PointMassSolver solver = new PointMassSolver(k);
        solver.setVerbose(false);
        solver.solve(mus);
        assertTrue(solver.isDiscrete());
        assertEquals(3, solver.getNumPoints());
        assertEquals(
                100,
                ms.invert(solver.getPoints()[0].loc),
                1e-5);
        assertEquals(0.0, solver.getCDF(ms.convert(50)), 1e-10);
        assertEquals(1.0, solver.getCDF(ms.convert(350)), 1e-10);
        assertEquals(1./3, solver.getCDF(ms.convert(150)), 1e-10);
    }

    @Test
    public void testUniform() {
        int k = 9;
        int n = 10000;
        double[] xs = new double[n+1];
        for (int i = 0; i < n+1; i++){
            xs[i] = i;
        }
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(k);
        ms.add(xs);
        double[] mus = ms.getPowerMoments();
        PointMassSolver solver = new PointMassSolver(k);
        solver.setVerbose(false);
        int numTrials = 1;
        long startTime = System.nanoTime();
        for (int i = 0; i < numTrials; i++) {
            solver.solve(mus);
        }
        long elapsed = System.nanoTime() - startTime;
        double p50 = ms.invert(solver.getQuantile(.5));
        assertTrue(p50 > n/2 - n/50);
        assertTrue(p50 < n/2 + n/50);
        assertEquals(.5, solver.getCDF(ms.convert(n/2)), .01);
    }

    @Test
    public void testExponential() {
        double[] pSumsAll = {
                300000.0,
                225966.42193620058,
                264605.9459043574,
                379946.31540360727,
                618057.4514014232,
                1097747.573452092,
                2085350.837861387,
                4182357.6214930336,
                8777836.388276778,
                19156041.219520878,
                43260076.99003405,
                100717617.54169427,
                241023513.17219356,
                591405293.3460146,
                1484890872.6219025
        };
        double[] range = {
                1.1336089478937639e-05, 3.3544939155892766
        };
        int k = 9;
        double[] pSums = Arrays.copyOf(pSumsAll, k);
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(range[0], range[1], pSums);
        PointMassSolver solver = new PointMassSolver(k);
        solver.setVerbose(false);
        double[] p_mus = ms.getPowerMoments();
        solver.solve(p_mus);
        assertEquals(.7, ms.invert(solver.getQuantile(.5)), .1);
    }

    @Test
    public void testGaussian() {
        double[] pSumsAll = {
                851.0, 2241.261731131048, 5919.499795417793, 15677.977395660744,
                41638.07512524008, 110884.21451481624, 296080.70121092687, 792673.9691043814,
                2127678.859911787
       };
        double[] range = {
                2.153035101484229, 3.0620760413639414
        };
        int k = 9;
        double[] pSums = Arrays.copyOf(pSumsAll, k);
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(range[0], range[1], pSums);
        PointMassSolver solver = new PointMassSolver(k);
        solver.setVerbose(false);
        double[] p_mus = ms.getPowerMoments();
        solver.solve(p_mus);
        assertEquals(
                .65,
                solver.getCDF(ms.convert(7.4)),
                .1
        );
    }

    @Test
    public void testHybrid() {
        int k = 9;
        int n = 10000;
        double[] xs = new double[2*n];
        for (int i = 0; i < n; i++){
            xs[i] = i;
        }
        for (int i = n; i < 2*n; i++) {
            xs[i] = 0;
        }
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(k);
        ms.add(xs);
        double[] mus = ms.getPowerMoments();
        PointMassSolver solver = new PointMassSolver(k);
        solver.setVerbose(false);
        int numTrials = 1;
        long startTime = System.nanoTime();
        for (int i = 0; i < numTrials; i++) {
            solver.solve(mus);
        }
        long elapsed = System.nanoTime() - startTime;
//        System.out.println("Time: "+elapsed*1e-9/numTrials);

        double p75 = ms.invert(solver.getQuantile(.75));
        assertTrue(p75 > n/2 - n/50);
        assertTrue(p75 < n/2 + n/50);
    }
}
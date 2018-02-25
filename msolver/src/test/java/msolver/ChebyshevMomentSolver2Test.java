package msolver;

import msolver.data.*;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ChebyshevMomentSolver2Test {
    @Test
    public void testGetCDF() {
        MomentData data = new ExponentialData();
        double[] range = {data.getMin(), data.getMax()};
        double[] logRange = {data.getLogMin(), data.getLogMax()};
        double[] powerSums = data.getPowerSums(9);
        double[] logSums = data.getLogSums(9);

        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                logRange[0], logRange[1], logSums
        );
        solver.solve(1e-9);

        double cdf = solver.estimateCDF(4);
        assertEquals(0.98, cdf, 0.01);
    }

    @Test
    public void testExpMix() {
        double[] range = {0.00016239676113615254, 11.080746252112679, -8.7254680741784778, 2.4052090303248499};
        double[] powerSums = {10000.0, 15132.848519412149, 60833.256862142742, 364446.70116487902, 2516373.8493943713};
//        double[] logSums = {10000.0, -3545.2047275402342, 21064.084600982864, -42730.224929083415, 222739.56454899674};
        double[] logSums = {10000.0};
        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                range[2], range[1], logSums
        );
        solver.solve(1e-9);
        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
    }


    @Test
    public void testMilan() {
        MomentData data = new MilanData();
        double[] range = {data.getMin(), data.getMax()};
        double[] logRange = {data.getLogMin(), data.getLogMax()};
        double[] powerSums = data.getPowerSums(9);
        double[] logSums = data.getLogSums(9);

        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                logRange[0], logRange[1], logSums
        );
        solver.solve(1e-9);
        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
        assertEquals(3.0, qs[1], 1.0);
        assertEquals(476.0, qs[3], 150.0);
    }

    @Test
    public void testDruid() {
        double[] range = {2.331497699529331E-6,7936.265379884158};
        double[] logRange = new double[2];
        logRange[0] = Math.log(range[0]);
        logRange[1] = Math.log(range[1]);

        double[] powerSums = {1.2814767E7, 4.6605082350179493E8, 1.309887742552026E11, 1.0548055486726956E14, 1.74335364401727808E17, 4.676320625451096E20, 1.713914827616506E24, 7.732420316935072E27};
        double[] logSums = {1.2814767E7, 8398321.384180075, 1.5347415740933093E8, 9.186643473957856E7, 3.4859819726620092E9, -2.9439576163248196E9, 1.2790650864340628E11, -4.578233220122189E11};

        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                logRange[0], logRange[1], logSums
        );
        solver.solve(1e-9);
        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
    }

    @Test
    public void testRetail() {
        MomentData data = new RetailQuantityData();
        double[] range = {data.getMin(), data.getMax()};
        double[] logRange = {data.getLogMin(), data.getLogMax()};
        double[] powerSums = data.getPowerSums(7);
        double[] logSums = data.getLogSums(7);

        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                logRange[0], logRange[1], logSums
        );
        solver.solve(1e-9);
        double[] ps = {.1, .5, .9};
        double[] qs = solver.estimateQuantiles(ps);
        assertEquals(3.5, qs[1], 1);
    }

    @Test
    public void testMilanSlow2() {
        double[] range = {0.0660641515826, 1132.45333683};
        double[] powerSums = {6400.0, 2029906.399774176, 1.0429918877324446E9, 5.780038122901864E11, 3.416250946008891E14, 2.14240501430776544E17, 1.4239668751562714E20, 1.0041004985137054E23, 7.519721016948273E25};
        double[] logSums = {6400.0, 24291.716240560112, 166411.34767031836, 976357.261249677, 6121315.203662701, 3.7448917881558396E7, 2.324092899690887E8, 1.4384302798955774E9, 8.943507161577347E9};
        powerSums = Arrays.copyOf(powerSums,9);
        logSums = Arrays.copyOf(logSums,9);
        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                Math.log(range[0]), Math.log(range[1]), logSums
        );
        solver.solve(1e-9);

        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
    }

    @Test
    public void testMilanSlow() {
        double[] range = {0.00442444627252, 1030.48397461};
        double[] powerSums = {7192.0, 389385.66361174756, 4.1719567742580794E7, 5.97001526023661E9, 1.7825854804116147E12, 1.2591880890700855E15, 1.21322134401215718E18, 1.2367230447262056E21, 1.2720770530673794E24};
        double[] logSums = {7192.0, 14780.48271228574, 92154.58769732877,     367651.48292626103, 1758410.8301197188, 7783606.849054701, 3.66666840287128E7, 1.661340608663317E8, 7.951549959965111E8};
        powerSums = Arrays.copyOf(powerSums,9);
        logSums = Arrays.copyOf(logSums,9);
        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                Math.log(range[0]), Math.log(range[1]), logSums
        );
        solver.solve(1e-9);

        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
    }

    @Test
    public void testWikiSlow2() {
        double[] range = {1.0, 365018.0};
        double[] logRange = {0.0, 12.807701946417174};
        double[] powerSums = {15390.0, 2.993478E7, 1.104169071664E12, 9.4489132972188032E16, 2.119775535058452E22};
        double[] logSums = {15390.0, 64231.9185887806, 367766.1421633075, 2487583.9684920274, 1.8934513580824606E7};
        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                logRange[0], logRange[1], logSums
        );
        solver.solve(1e-9);
        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
    }

    @Test
    public void testWikiSlow() {
        double[] range = {1.0, 738264.0};
        double[] logRange = {0.0, 13.512056763192021};
        double[] powerSums = {53.0, 3782523.0, 2.725910937369E12, 2.0119007431824297E18, 1.4853089183589466E24};
        double[] logSums = {53.0, 315.25127662867254, 2490.551811125909, 23731.847485560018, 255843.41344724607};
        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                logRange[0], logRange[1], logSums
        );
        solver.solve(1e-9);
        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
    }

    @Test
    public void testOccupancy() {
        MomentData data = new OccupancyData();
        double[] range = {data.getMin(), data.getMax()};
        double[] logRange = {data.getLogMin(), data.getLogMax()};
        double[] powerSums = data.getPowerSums(7);
        double[] logSums = data.getLogSums(1);

        ChebyshevMomentSolver2 solver = ChebyshevMomentSolver2.fromPowerSums(
                range[0], range[1], powerSums,
                logRange[0], logRange[1], logSums
        );
        solver.solve(1e-9);
        double[] ps = {.1, .5, .9, .99};
        double[] qs = solver.estimateQuantiles(ps);
        assertEquals(565.0, qs[1], 7.0);
    }
}
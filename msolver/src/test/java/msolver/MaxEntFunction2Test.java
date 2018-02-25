package msolver;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MaxEntFunction2Test {
    @Test
    public void testSimple() {
        double[] aCoeffs = {0, -1};
        double[] bCoeffs = {0, 1};
        MaxEntFunction2 f = new MaxEntFunction2(
                true,
                aCoeffs,
                bCoeffs,
                5.05,
                4.95,
                2.220446049250313e-16,
                2.302585092994046
        );
        assertEquals(3.73002156214, f.zerothMoment(1e-8), 1e-8);

        double[][] pairwiseMoments = f.getPairwiseMoments(1e-8);
        assertEquals(3.73002156214, pairwiseMoments[0][0], 1e-8);
        assertEquals(0.4078218803, pairwiseMoments[1][3], 1e-8);
        assertEquals(1.12095675177, pairwiseMoments[1][1], 1e-8);

        double[][] hess = f.getHessian(1e-7);
        assertEquals(3.73002156214, hess[0][0], 1e-8);
        assertEquals(0.4078218803, hess[1][2], 1e-8);
        assertEquals(1.12095675177, hess[1][1], 1e-8);
    }

    @Test
    public void testCompareNumpy(){
        double[] aCoeffs = {-1495.2106196044201, 63797.93868346012, -830014.2179376424, -296736.79198347515, -56032.24104079366, -6481.702314031079, -394.9593472527941};
        double[] bCoeffs = {0, 280793.4251573418, 887242.8040778289, -41598.46235869913, 725.9088731130822, 223.10996965225195, -35.8575946806040};
        MaxEntFunction2 f = new MaxEntFunction2(
                false,
                aCoeffs,
                bCoeffs,
                6.830640572935523, 0.8077984901352853, 1244.625000, 831.875000
        );
        assertEquals(1.23, f.value(-.8), 0.01);
    }
}
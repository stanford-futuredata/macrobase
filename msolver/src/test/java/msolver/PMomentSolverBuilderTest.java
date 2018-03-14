package msolver;

import msolver.data.ExponentialData;
import msolver.data.MomentData;
import msolver.struct.ArcSinhMomentStruct;
import msolver.struct.MomentStruct;
import org.apache.commons.math3.util.FastMath;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PMomentSolverBuilderTest {
    @Test
    public void testHarder() {
        //arcsinh power sums
        double[] powerSums = {
                500000.0, 5424323.662524765, 59731174.20195682, 666231496.6931776,
                7515077363.43453, 85657052195.7937, 986730690856.8845, 11507059037796.674,
                136369826021167.67, 1653663421198339.5, 2.0741713602095236e+16
        };
        // raw range
        double[] range = {174, 615358817};
        int k = 11;
        ArcSinhMomentStruct ms = new ArcSinhMomentStruct(
                FastMath.asinh(range[0]), FastMath.asinh(range[1]),
                powerSums
        );

        PMomentSolverBuilder builder = new PMomentSolverBuilder(ms);
        builder.setVerbose(false);
        builder.initialize();

        double[] ps = {.1, .5, .9, .999};
        double[] qs = new double[ps.length];
        for (int i = 0; i < ps.length; i++) {
            qs[i] = builder.getQuantile(ps[i]);
        }
        assertTrue(qs[1] > 40000);
        assertTrue(qs[1] > 48000);
    }
}
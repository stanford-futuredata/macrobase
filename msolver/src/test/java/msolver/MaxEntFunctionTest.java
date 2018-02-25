package msolver;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MaxEntFunctionTest {
    @Test
    public void testMoments() {
        double[] coeff = {1.0, 2.0, 3.0};
        MaxEntFunction f = new MaxEntFunction(coeff);
        double[] moments = f.moments(8, 1e-9);
        double[] expectedMoments = {
            6.303954641290793, -1.0395877292934701,
            -4.9297352972133845, 2.0119170973456093,
            2.458369282294647, -1.5127916121976486,
            -0.84272224125321182, 0.73491729283435847
        };
        for (int i = 0; i < moments.length; i++) {
            assertEquals(expectedMoments[i], moments[i], 1e-10);
        }
        assertTrue(f.getFuncEvals() < 1000);
    }
}
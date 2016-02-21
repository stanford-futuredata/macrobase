package macrobase.analysis.outlier;

import static org.junit.Assert.*;

import java.util.ArrayList;

import macrobase.datamodel.TimeDatum;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

public class MovingAverageTest {
    @Test
    public void simpleTest() {
        MovingAverage ma = new MovingAverage(3);
        assertEquals(0, ma.score(createDatum(0, 1)), 0);
        assertEquals(1/3.0, ma.score(createDatum(1, 2)), 1e-5); // Average = 1.5
        assertEquals(1/2.0, ma.score(createDatum(2, 3)), 1e-5); // Average = 2
        assertEquals(1/3.0, ma.score(createDatum(3, 4)), 1e-5); // Average = 3
    }
    
    private TimeDatum createDatum(int time, double d) {
        double[] sample = new double[1];
        sample[0] = d;
        return new TimeDatum(time, new ArrayList<>(), new ArrayRealVector(sample));
    }
    
    @Test
    public void weightTest() {
        MovingAverage ma = new MovingAverage(3);
        assertEquals(0, ma.score(createDatum(0, 1)), 0);
        assertEquals(1/3.0, ma.score(createDatum(1, 2)), 0); // Average = 1.5
        assertEquals(1/3.0, ma.score(createDatum(3, 3)), 0); // This point has twice the weight due to its time, so average = 2.25
    }
}

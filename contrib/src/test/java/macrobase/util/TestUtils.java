package macrobase.util;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;

import java.util.ArrayList;

public class TestUtils {
    public static Datum createTimeDatum(int time, double d) {
        double[] sample = new double[2];
        sample[0] = time;
        sample[1] = d;
        return new Datum(new ArrayList<>(), new ArrayRealVector(sample));
    }
}

package macrobase.analysis.outlier;

import java.util.ArrayList;

import macrobase.datamodel.TimeDatum;

import org.apache.commons.math3.linear.ArrayRealVector;

public class TestOutlierUtils {
    public static TimeDatum createTimeDatum(int time, double d) {
        double[] sample = new double[1];
        sample[0] = d;
        return new TimeDatum(time, new ArrayList<>(), new ArrayRealVector(sample));
    }
}

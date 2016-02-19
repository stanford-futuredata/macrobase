package macrobase.analysis.outlier;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class KDETest {

    @Test
    public void simpleTest() {
        KDE kde = new KDE(KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE, KDE.BandwidthAlgorithm.OVERSMOOTHED);
        kde.setProportionOfDataToUse(1.0);
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        kde.train(data);
        assertEquals(kde.score(data.get(0)), -0.00513, 1e-5);
        assertEquals(kde.score(data.get(50)), -0.009997, 1e-5);
        assertEquals(kde.score(data.get(data.size() - 1)), -0.005132, 1e-5);
    }

}

package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class BinnedKDETest {

    @Test
    public void simpleTest() {
        MacroBaseConf conf = new MacroBaseConf()
                .set(MacroBaseConf.KDE_KERNEL_TYPE, "EPANECHNIKOV_MULTIPLICATIVE")
                .set(MacroBaseConf.KDE_BANDWIDTH_ALGORITHM, "OVERSMOOTHED")
                .set(MacroBaseConf.BINNED_KDE_BINS, 1000);
        KDE kde = new macrobase.analysis.stats.BinnedKDE(conf);
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        kde.train(data);
        assertEquals( -0.005129, kde.score(data.get(0)), 1e-5);
        assertEquals(-0.010001, kde.score(data.get(50)),  1e-5);
        assertEquals(-0.005133, kde.score(data.get(data.size() - 1)), 1e-5);
    }
}

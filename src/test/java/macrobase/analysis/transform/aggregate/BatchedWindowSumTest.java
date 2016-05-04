package macrobase.analysis.transform.aggregate;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BatchedWindowSumTest {
    private boolean almostEqual(double actual, double expected) {
        return Math.abs(actual - expected) < 1e-5;
    }

    @Test
    public void testBatchedWindowSum() throws Exception {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 10; i ++) {
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(3));
            d.getMetrics().setEntry(0, i);
            d.getMetrics().setEntry(1, 1);
            d.getMetrics().setEntry(2, 1.0 / (i + 1));
            data.add(d);
        }

        BatchedWindowSum windowSum = new BatchedWindowSum();
        Datum sum = windowSum.process(data);
        assert(sum.getMetrics().getEntry(0) == 45);
        assert(sum.getMetrics().getEntry(1) == 10);
        assert(almostEqual(sum.getMetrics().getEntry(2), 7381.0 / 2520));

        /* Test datum with time column */
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        windowSum = new BatchedWindowSum(conf);
        sum = windowSum.process(data);
        assert(sum.getMetrics().getEntry(0) == 0);
        assert(sum.getMetrics().getEntry(1) == 10);
        assert(almostEqual(sum.getMetrics().getEntry(2), 7381.0 / 2520));
    }
}

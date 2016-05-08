package macrobase.analysis.transform.aggregate;

import macrobase.analysis.outlier.TestOutlierUtils;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BatchedWindowSumTest {
    private boolean almostEqual(double actual, double expected) {
        return Math.abs(actual - expected) < 1e-5;
    }

    @Test
    public void testWindowSum() throws Exception {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 10; i ++) {
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(3));
            d.getMetrics().setEntry(0, i);
            d.getMetrics().setEntry(1, 1);
            d.getMetrics().setEntry(2, 1.0 / (i + 1));
            data.add(d);
        }

        BatchedWindowSum windowSum = new BatchedWindowSum();
        Datum sum = windowSum.aggregate(data);
        assert(sum.getMetrics().getEntry(0) == 45);
        assert(sum.getMetrics().getEntry(1) == 10);
        assert(almostEqual(sum.getMetrics().getEntry(2), 7381.0 / 2520));

        /* Test datum with time column */
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        windowSum = new BatchedWindowSum(conf);
        sum = windowSum.aggregate(data);
        assert(sum.getMetrics().getEntry(0) == 0);
        assert(sum.getMetrics().getEntry(1) == 10);
        assert(almostEqual(sum.getMetrics().getEntry(2), 7381.0 / 2520));
    }

    @Test
    public void testWindowUpdate() throws Exception {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            Datum d = TestOutlierUtils.createTimeDatum(i, i);
            data.add(d);
        }

        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        BatchedWindowSum windowSum = new BatchedWindowSum(conf);
        // First window
        Datum sum = windowSum.updateWindow(data.subList(0, 10), new ArrayList<>());
        assert(sum.getMetrics().getEntry(1) == 45);
        // Update window
        sum = windowSum.updateWindow(data.subList(10, 11), data.subList(0, 1));
        assert(sum.getMetrics().getEntry(1) == 55);

        sum = windowSum.updateWindow(data.subList(11, 20), data.subList(1, 5));
        assert(sum.getMetrics().getEntry(1) == 180);
    }
}

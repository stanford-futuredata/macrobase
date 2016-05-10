package macrobase.analysis.transform.aggregate;

import macrobase.analysis.outlier.TestOutlierUtils;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BatchedWindowCountTest {
    @Test
    public void testBatchedWindowCount() throws Exception {
        Random r = new Random(0);
        int size = Math.abs(r.nextInt()) % 100 + 1;
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < size; i ++) {
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(3));
            d.getMetrics().setEntry(0, i);
            d.getMetrics().setEntry(1, 1);
            d.getMetrics().setEntry(2, -i);
            data.add(d);
        }

        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        BatchedWindowCount windowCount = new BatchedWindowCount(conf);
        Datum count = windowCount.aggregate(data);
        assert(count.getMetrics().getDimension() == 3);
        assert(count.getMetrics().getEntry(1) == size);
        assert(count.getMetrics().getEntry(2) == size);
    }

    @Test
    public void testWindowUpdate() throws Exception {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            Datum d = TestOutlierUtils.createTimeDatum(i, i);
            data.add(d);
        }

        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        BatchedWindowCount windowCount = new BatchedWindowCount(conf);
        // First window
        Datum count = windowCount.updateWindow(data.subList(0, 10), new ArrayList<>());
        assert(count.getMetrics().getEntry(1) == 10);
        // Update window
        count = windowCount.updateWindow(data.subList(10, 11), data.subList(0, 1));
        assert(count.getMetrics().getEntry(1) == 10);

        count = windowCount.updateWindow(data.subList(11, 20), data.subList(1, 5));
        assert(count.getMetrics().getEntry(1) == 15);
    }
}
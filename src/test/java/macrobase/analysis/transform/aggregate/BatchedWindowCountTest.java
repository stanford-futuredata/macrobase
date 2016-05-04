package macrobase.analysis.transform.aggregate;

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

        BatchedWindowCount windowCount = new BatchedWindowCount();
        Datum count = windowCount.process(data);
        assert(count.getMetrics().getDimension() == 1);
        assert(count.getMetrics().getEntry(0) == size);

        /* Test datum with time column */
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        windowCount = new BatchedWindowCount(conf);
        count = windowCount.process(data);
        assert(count.getMetrics().getDimension() == 3);
        assert(count.getMetrics().getEntry(0) == 0);
        assert(count.getMetrics().getEntry(1) == size);
        assert(count.getMetrics().getEntry(2) == size);
    }
}

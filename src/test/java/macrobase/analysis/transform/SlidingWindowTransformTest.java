package macrobase.analysis.transform;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.analysis.outlier.TestOutlierUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class SlidingWindowTransformTest {
    @Test
    public void testSlidingWindowTransform() throws Exception {
        List<Datum> data = new ArrayList<>();
        for(int i = 0; i < 10; ++i) {
            Datum d = TestOutlierUtils.createTimeDatum(i, i);
            data.add(d);
        }

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        SlidingWindowTransform sw = new SlidingWindowTransform(conf, 3);
        sw.consume(data);
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        for (int i = 0; i < 4; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 3);
            if (i == 3)
                assertTrue(d.getMetrics().getEntry(1) == 1);
            else
                assertTrue(d.getMetrics().getEntry(1) == 3);

        }

        /* Test a different aggregation function */
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        sw = new SlidingWindowTransform(conf, 3);
        sw.consume(data);
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        for (int i = 0; i < 4; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 3);
            if (i == 3)
                assertTrue(d.getMetrics().getEntry(1) == 9);
            else
                assertTrue(d.getMetrics().getEntry(1) == (i * 3 + 1) * 3);
        }

        /* Test a different pane size */
        sw = new SlidingWindowTransform(conf, 5);
        sw.consume(data);
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 2);
        for (int i = 0; i < 2; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 5);
            assertTrue(d.getMetrics().getEntry(1) == (i * 5 + 2) * 5);
        }

        // For test coverage
        sw.initialize();
        sw.shutdown();
    }

}

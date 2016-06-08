package macrobase.analysis.transform.aggregate;

import macrobase.analysis.TestUtils;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class IncrementalWindowSumTest {
    @Test
    public void testWindowUpdate() throws Exception {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Datum d = TestUtils.createTimeDatum(i, i);
            data.add(d);
        }

        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        IncrementalWindowSum windowSum = new IncrementalWindowSum(conf);
        // First window
        Datum sum = windowSum.updateWindow(data.subList(0, 10), new ArrayList<>());
        assert(sum.getMetrics().getEntry(1) == 45);
        // Update window
        sum = windowSum.updateWindow(data.subList(10, 11), data.subList(0, 1));
        assert(sum.getMetrics().getEntry(1) == 55);
        sum = windowSum.updateWindow(data.subList(11, 20), data.subList(1, 5));
        assert(sum.getMetrics().getEntry(1) == 180);
        sum = windowSum.updateWindow(new ArrayList<>(), data.subList(5, 10));
        assert(sum.getMetrics().getEntry(1) == 145);
    }
}

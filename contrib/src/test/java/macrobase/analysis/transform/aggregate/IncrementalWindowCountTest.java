package macrobase.analysis.transform.aggregate;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class IncrementalWindowCountTest {
    @Test
    public void testWindowUpdate() throws Exception {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            Datum d = TestUtils.createTimeDatum(i, i);
            data.add(d);
        }

        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        IncrementalWindowCount windowCount = new IncrementalWindowCount(conf);
        // First window
        Datum count = windowCount.updateWindow(data.subList(0, 10), new ArrayList<>());
        assert(count.metrics().getEntry(1) == 10);
        // Update window
        count = windowCount.updateWindow(data.subList(10, 11), data.subList(0, 1));
        assert(count.metrics().getEntry(1) == 10);
        count = windowCount.updateWindow(data.subList(11, 20), data.subList(1, 5));
        assert(count.metrics().getEntry(1) == 15);
        count = windowCount.updateWindow(new ArrayList<>(), data.subList(5, 10));
        assert(count.metrics().getEntry(1) == 10);
    }
}

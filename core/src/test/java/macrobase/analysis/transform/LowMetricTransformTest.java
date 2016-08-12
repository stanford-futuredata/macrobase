package macrobase.analysis.transform;

import macrobase.datamodel.Datum;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class LowMetricTransformTest {
    @Test
    public void testLowMetricTransform() throws Exception {
        List<Datum> data = new ArrayList<>();
        data.add(new Datum(new ArrayList<>(), 1, 2));
        data.add(new Datum(new ArrayList<>(), 1, .01));

        List<Integer> targetAttrs = new ArrayList<>();
        targetAttrs.add(1);

        LowMetricTransform lmt = new LowMetricTransform(targetAttrs);
        lmt.consume(data);
        data = lmt.getStream().drain();

        assertEquals(2, data.size(), 0);
        assertEquals(1, data.get(0).metrics().getEntry(0), 0);
        assertEquals(.5, data.get(0).metrics().getEntry(1), 0);
        assertEquals(1, data.get(1).metrics().getEntry(0), 0);
        assertEquals(10, data.get(1).metrics().getEntry(1), 0);
    }
}

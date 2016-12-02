package macrobase.analysis.pipeline.stream;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class TimeDatumStreamTest {
    private static TimeDatumStream stream;
    private static MacroBaseConf conf = new MacroBaseConf();
    private static List<Datum> data = new ArrayList<>();

    @Test
    public void testDrainDuration() {
        conf.set(MacroBaseConf.TIME_COLUMN, 0);

        for (int i = 0; i < 100; ++i) {
            Datum d = TestUtils.createTimeDatum(2 * i, 100 - i);
            data.add(d);
        }

        stream = new TimeDatumStream(data, 0);
        List<Datum> result = stream.drainDuration(1);
        assertTrue(result.size() == 1);
        result = stream.drainDuration(100);
        assertTrue(result.size() == 50);
        result = stream.drainDuration(200);
        assertTrue(result.size() == 49);
    }
}

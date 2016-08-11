package macrobase.analysis.transform;

import macrobase.analysis.outlier.TestOutlierUtils;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class BatchSWTransformTest {
    private static MacroBaseConf conf = new MacroBaseConf();
    private static List<Datum> data = new ArrayList<>();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.MAX);

        for (int i = 0; i < 100; ++i) {
            Datum d = TestOutlierUtils.createTimeDatum(i, 100 - i);
            data.add(d);
        }
    }

    @Test
    public void testBasicMaxAggregate() throws Exception {
        conf.set(MacroBaseConf.WINDOW_SIZE, 10);
        SlidingWindowTransform sw = new BatchSWTransform(conf, 5);
        sw.initialize();
        sw.consume(data.subList(0, 20));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        for (int i = 0; i < 3; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 5);
            assertTrue(d.getMetrics().getEntry(1) == 100 - 5 * i);
        }
    }

    private void testContinuousStreams(int stream1, int stream2) throws Exception {
        SlidingWindowTransform sw = new BatchSWTransform(conf, 25);
        sw.initialize();
        sw.consume(data.subList(0, stream1));
        sw.consume(data.subList(stream1, stream2));
        sw.consume(data.subList(stream2, 100));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        for (int i = 0; i < 4; i++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 25);
            assertTrue(d.getMetrics().getEntry(1) == 100 - 25 * i);
        }
    }

    private void testDiscontinuousStreams() throws Exception {
        SlidingWindowTransform sw = new BatchSWTransform(conf, 25);
        // Should produce empty window in between
        sw.consume(data.subList(0, 46));
        sw.consume(data.subList(80, 100));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 0);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 100);
        assertTrue(transformed.get(1).getMetrics().getEntry(0) == 25);
        assertTrue(transformed.get(1).getMetrics().getEntry(1) == 75);
        assertTrue(transformed.get(2).getMetrics().getEntry(0) == 50);
        assertTrue(transformed.get(2).getMetrics().getEntry(1) == 0);
        assertTrue(transformed.get(3).getMetrics().getEntry(0) == 75);
        assertTrue(transformed.get(3).getMetrics().getEntry(1) == 20);
    }

    @Test
    public void testStreaming() throws Exception {
        // window = 35, slide = 25, MAX
        conf.set(MacroBaseConf.WINDOW_SIZE, 35);
        // Test two different breakdowns of streams should get the same result
        testContinuousStreams(40, 85);
        testContinuousStreams(13, 57);
        // Test data streams that have gaps in between
        conf.set(MacroBaseConf.WINDOW_SIZE, 30);
        testDiscontinuousStreams();
    }

}

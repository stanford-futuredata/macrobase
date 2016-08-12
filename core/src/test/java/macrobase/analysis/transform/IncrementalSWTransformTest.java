package macrobase.analysis.transform;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class IncrementalSWTransformTest {
    private static MacroBaseConf conf = new MacroBaseConf();
    private static List<Datum> data = new ArrayList<>();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf.set(MacroBaseConf.TIME_COLUMN, 0);

        for (int i = 0; i < 100; ++i) {
            Datum d = TestUtils.createTimeDatum(i, i);
            data.add(d);
        }
    }

    @Test
    public void testBasicCountAggregate() throws Exception {
        conf.set(MacroBaseConf.WINDOW_SIZE, 10);
        SlidingWindowTransform sw = new IncrementalSWTransform(conf, 5);
        sw.initialize();
        sw.consume(data.subList(0, 20));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        for (int i = 0; i < 3; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 5);
            assertTrue(d.getMetrics().getEntry(1) == 10);
        }
    }

    @Test
    public void testBasicSumAggregate() throws Exception {
        conf.set(MacroBaseConf.WINDOW_SIZE, 10);
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        SlidingWindowTransform sw = new IncrementalSWTransform(conf, 5);
        sw.initialize();
        sw.consume(data.subList(0, 20));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        for (int i = 0; i < 3; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 5);
            assertTrue(d.getMetrics().getEntry(1) == 45 + 50 * i);
        }
    }

    private void testContinuousStreams(int stream1, int stream2) throws Exception {
        SlidingWindowTransform sw = new IncrementalSWTransform(conf, 25);
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
            if (i == 3)
                assertTrue(d.getMetrics().getEntry(1) == 25);
            else
                assertTrue(d.getMetrics().getEntry(1) == 35);
        }
    }

    private void testDiscontinuousStreams() throws Exception {
        SlidingWindowTransform sw = new IncrementalSWTransform(conf, 25);
        // Should produce empty window in between
        sw.consume(data.subList(0, 46));
        sw.consume(data.subList(80, 100));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 0);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 30);
        assertTrue(transformed.get(1).getMetrics().getEntry(0) == 25);
        assertTrue(transformed.get(1).getMetrics().getEntry(1) == 21);
        assertTrue(transformed.get(2).getMetrics().getEntry(0) == 50);
        assertTrue(transformed.get(2).getMetrics().getEntry(1) == 0);
        assertTrue(transformed.get(3).getMetrics().getEntry(0) == 75);
        assertTrue(transformed.get(3).getMetrics().getEntry(1) == 20);
    }

    @Test
    public void testStreaming() throws Exception {
        // window = 35, slide = 25, COUNT
        conf.set(MacroBaseConf.WINDOW_SIZE, 35);
        // Test two different breakdowns of streams should get the same result
        testContinuousStreams(40, 85);
        testContinuousStreams(13, 57);
        // Test data streams that have gaps in between
        conf.set(MacroBaseConf.WINDOW_SIZE, 30);
        testDiscontinuousStreams();
    }

}

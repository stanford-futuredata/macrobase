package macrobase.analysis.transform;

import macrobase.analysis.transform.aggregate.AggregateConf;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class BatchSlidingWindowTransformTest {
    private static MacroBaseConf conf = new MacroBaseConf();
    private static List<Datum> data = new ArrayList<>();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        conf.set(AggregateConf.AGGREGATE_TYPE, AggregateConf.AggregateType.MAX);

        for (int i = 0; i < 100; ++i) {
            Datum d = TestUtils.createTimeDatum(i, 100 - i);
            data.add(d);
        }
    }

    @Test
    public void testBasicMaxAggregate() throws Exception {
        conf.set(MacroBaseConf.TIME_WINDOW, 10);
        SlidingWindowTransform sw = new BatchSlidingWindowTransform(conf, 5);
        sw.initialize();
        sw.consume(data.subList(0, 20));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        for (int i = 0; i < 3; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.metrics().getEntry(0) == i * 5);
            assertTrue(d.metrics().getEntry(1) == 100 - 5 * i);
        }
    }

    private void testContinuousStreams(int stream1, int stream2) throws Exception {
        SlidingWindowTransform sw = new BatchSlidingWindowTransform(conf, 25);
        sw.initialize();
        sw.consume(data.subList(0, stream1));
        sw.consume(data.subList(stream1, stream2));
        sw.consume(data.subList(stream2, 100));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        for (int i = 0; i < 4; i++) {
            Datum d = transformed.get(i);
            assertTrue(d.metrics().getEntry(0) == i * 25);
            assertTrue(d.metrics().getEntry(1) == 100 - 25 * i);
        }
    }

    private void testDiscontinuousStreams() throws Exception {
        SlidingWindowTransform sw = new BatchSlidingWindowTransform(conf, 25);
        // Should produce empty window in between
        sw.consume(data.subList(0, 46));
        sw.consume(data.subList(80, 100));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        assertTrue(transformed.get(0).metrics().getEntry(0) == 0);
        assertTrue(transformed.get(0).metrics().getEntry(1) == 100);
        assertTrue(transformed.get(1).metrics().getEntry(0) == 25);
        assertTrue(transformed.get(1).metrics().getEntry(1) == 75);
        assertTrue(transformed.get(2).metrics().getEntry(0) == 50);
        assertTrue(transformed.get(2).metrics().getEntry(1) == 0);
        assertTrue(transformed.get(3).metrics().getEntry(0) == 75);
        assertTrue(transformed.get(3).metrics().getEntry(1) == 20);
    }

    @Test
    public void testStreaming() throws Exception {
        // window = 35, slide = 25, MAX
        conf.set(MacroBaseConf.TIME_WINDOW, 35);
        // Test two different breakdowns of streams should get the same result
        testContinuousStreams(40, 85);
        testContinuousStreams(13, 57);
        // Test data streams that have gaps in between
        conf.set(MacroBaseConf.TIME_WINDOW, 30);
        testDiscontinuousStreams();
    }

}

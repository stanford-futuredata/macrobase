package macrobase.analysis.transform;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.analysis.outlier.TestOutlierUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class SlidingWindowTransformTest {
    private static MacroBaseConf conf = new MacroBaseConf();
    private static List<Datum> data = new ArrayList<>();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf.set(MacroBaseConf.TIME_COLUMN, 0);

        for (int i = 0; i < 100; ++i) {
            Datum d = TestOutlierUtils.createTimeDatum(i, 1);
            data.add(d);
        }
    }

    @Test
    public void testCountAggregate() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        conf.set(MacroBaseConf.SLIDE_SIZE, 3);
        SlidingWindowTransform sw = new SlidingWindowTransform(conf, 3);
        sw.initialize();
        sw.consume(data.subList(0, 10));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        for (int i = 0; i < 4; i ++) {
            Datum d = transformed.get(i);
            if (i == 3)
                assertTrue(d.getMetrics().getEntry(0) == 7);
            else
                assertTrue(d.getMetrics().getEntry(0) == i * 3);
            assertTrue(d.getMetrics().getEntry(1) == 3);
        }
    }

    @Test
    public void testSumAggregate() throws Exception {
        conf.set(MacroBaseConf.SLIDE_SIZE, 4);
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        SlidingWindowTransform sw = new SlidingWindowTransform(conf, 4);
        sw.initialize();
        sw.consume(data.subList(0, 12));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        for (int i = 0; i < 3; i ++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 4);
            assertTrue(d.getMetrics().getEntry(1) == 4);
        }
    }

    private void testWindowSlide() throws Exception {
        // window = 20, slide = 10
        SlidingWindowTransform sw = new SlidingWindowTransform(conf, 20);
        sw.initialize();
        sw.consume(data.subList(0, 50));
        sw.shutdown();
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        for (int i = 0; i < 4; i++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 10);
            assertTrue(d.getMetrics().getEntry(1) == 20);
        }

        /* window = 15, slide = 10 */
        sw = new SlidingWindowTransform(conf, 15);
        sw.initialize();
        sw.consume(data.subList(0, 50));
        /* Without flushing we should expected the windows to start from 0, 10, 20, 30 */
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        for (int i = 0; i < 4; i++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 10);
            assertTrue(d.getMetrics().getEntry(1) == 15);
        }
        sw.shutdown();
        /* Flushing will get us the last window that starts at 35 */
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 1);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 35);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 15);
    }

    @Test
    public void testOnePassData() throws Exception {
        conf.set(MacroBaseConf.SLIDE_SIZE, 10);
        testWindowSlide();
        // Test the same setting with pane aggregation
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        testWindowSlide();
    }

    private void testContinuousStreams(int stream1, int stream2) throws Exception {
        SlidingWindowTransform sw = new SlidingWindowTransform(conf, 35);
        sw.initialize();
        sw.consume(data.subList(0, stream1));
        sw.consume(data.subList(stream1, stream2));
        sw.consume(data.subList(stream2, 100));
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        for (int i = 0; i < 3; i++) {
            Datum d = transformed.get(i);
            assertTrue(d.getMetrics().getEntry(0) == i * 25);
            assertTrue(d.getMetrics().getEntry(1) == 35);
        }
        // Force flush the last window
        sw.shutdown();
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 1);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 65);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 35);
    }

    private void testDiscontinuousStreams() throws Exception {
        SlidingWindowTransform sw = new SlidingWindowTransform(conf, 35);
        // Test the case when we should flush the first stream,
        // and carry over the window to the second stream
        sw.consume(data.subList(0, 46));
        sw.consume(data.subList(70, 100));
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 4);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 0);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 35);
        assertTrue(transformed.get(1).getMetrics().getEntry(0) == 11);
        assertTrue(transformed.get(1).getMetrics().getEntry(1) == 35);
        assertTrue(transformed.get(2).getMetrics().getEntry(0) == 36);
        assertTrue(transformed.get(2).getMetrics().getEntry(1) == 11);
        assertTrue(transformed.get(3).getMetrics().getEntry(0) == 61);
        assertTrue(transformed.get(3).getMetrics().getEntry(1) == 26);
        sw.shutdown();
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 1);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 65);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 30);

        // Test the case when we should start a new window for the second stream
        sw = new SlidingWindowTransform(conf, 30);
        sw.consume(data.subList(0, 44));
        sw.consume(data.subList(70, 100));
        sw.shutdown();
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 0);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 30);
        assertTrue(transformed.get(1).getMetrics().getEntry(0) == 14);
        assertTrue(transformed.get(1).getMetrics().getEntry(1) == 30);
        assertTrue(transformed.get(2).getMetrics().getEntry(0) == 70);
        assertTrue(transformed.get(2).getMetrics().getEntry(1) == 30);
    }

    private void testPaneDiscontinuousStreams() throws Exception {
        // Test the case when we should flush the first stream,
        // and carry over the window to the second stream
        SlidingWindowTransform sw = new SlidingWindowTransform(conf, 35);
        sw.consume(data.subList(0, 56));
        sw.consume(data.subList(70, 100));
        List<Datum> transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 0);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 35);
        assertTrue(transformed.get(1).getMetrics().getEntry(0) == 25);
        assertTrue(transformed.get(1).getMetrics().getEntry(1) == 31);
        assertTrue(transformed.get(2).getMetrics().getEntry(0) == 50);
        assertTrue(transformed.get(2).getMetrics().getEntry(1) == 21);
        sw.shutdown();
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 1);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 65);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 30);

        // Test the case when we should start a new window for the second stream
        sw = new SlidingWindowTransform(conf, 30);
        sw.consume(data.subList(0, 44));
        sw.consume(data.subList(70, 100));
        sw.shutdown();
        transformed = sw.getStream().drain();
        assertTrue(transformed.size() == 3);
        assertTrue(transformed.get(0).getMetrics().getEntry(0) == 0);
        assertTrue(transformed.get(0).getMetrics().getEntry(1) == 30);
        assertTrue(transformed.get(1).getMetrics().getEntry(0) == 15);
        assertTrue(transformed.get(1).getMetrics().getEntry(1) == 29);
        assertTrue(transformed.get(2).getMetrics().getEntry(0) == 70);
        assertTrue(transformed.get(2).getMetrics().getEntry(1) == 30);
    }

    @Test
    public void testStreaming() throws Exception {
        // window = 35, slide = 25, COUNT
        conf.set(MacroBaseConf.SLIDE_SIZE, 25);
        // Test two different breakdowns of streams should get the same result
        testContinuousStreams(40, 85);
        testContinuousStreams(13, 57);
        testDiscontinuousStreams();

        // Same setting, with pane aggregation
        conf.set(MacroBaseConf.AGGREGATE_TYPE, MacroBaseConf.AggregateType.SUM);
        testContinuousStreams(40, 85);
        testContinuousStreams(13, 57);
        testPaneDiscontinuousStreams();
    }
}

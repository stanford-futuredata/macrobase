package macrobase.pipeline.operator;

import com.google.common.collect.Lists;
import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.pipeline.stream.MBStream;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MBOperatorTest {
    private class PassThroughCountingOperator extends MBOperator<Integer, Integer> {
        private List<Integer> tupleCounts = new ArrayList<>();
        private MBStream<Integer> output = new MBStream<>();
        private boolean isInitialized = false;
        private boolean isShutdown = false;

        @Override
        public void initialize() throws Exception {
            isInitialized = true;
        }

        @Override
        public void consume(List<Integer> records) throws Exception {
            tupleCounts.add(records.size());
            output.add(records);
        }

        @Override
        public void shutdown() throws Exception {
            isShutdown = true;
        }

        @Override
        public MBStream<Integer> getStream() throws Exception {
            return output;
        }

        public List<Integer> getTupleCounts() {
            return tupleCounts;
        }

        public boolean isInitialized() {
            return isInitialized;
        }

        public boolean isShutdown() {
            return isShutdown;
        }
    }

    @Test
    public void testOperator() throws Exception {
        PassThroughCountingOperator o1 = new PassThroughCountingOperator();
        PassThroughCountingOperator o2 = new PassThroughCountingOperator();
        PassThroughCountingOperator o3 = new PassThroughCountingOperator();

        MBOperator<Integer, Integer> thru = o1.then(o2).then(o3);

        thru.initialize();
        assertTrue(o1.isInitialized());
        assertTrue(o2.isInitialized());
        assertTrue(o3.isInitialized());

        List<Integer> testData = new ArrayList<>();
        for(int i = 0; i < 510; ++i) {
            testData.add(i);
        }

        MBStream<Integer> inputStream = new MBStream<>(testData);

        final int BATCH_SIZE = 100;
        int prevSize = inputStream.remaining();

        while(inputStream.remaining() > 0) {
            thru.consume(inputStream.drain(BATCH_SIZE));

            if(inputStream.remaining() > 0) {
                assertEquals((int) inputStream.remaining(), prevSize - BATCH_SIZE);
            }

            prevSize = inputStream.remaining();
        }

        ArrayList<Integer> expected = Lists.newArrayList(100, 100, 100, 100, 100, 10);

        assertEquals(expected, o1.getTupleCounts());
        assertEquals(expected, o2.getTupleCounts());
        assertEquals(expected, o3.getTupleCounts());

        thru.shutdown();

        assertTrue(o1.isShutdown());
        assertTrue(o2.isShutdown());
        assertTrue(o3.isShutdown());
    }

}

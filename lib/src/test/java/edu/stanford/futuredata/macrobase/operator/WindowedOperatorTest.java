package edu.stanford.futuredata.macrobase.operator;

import edu.stanford.futuredata.macrobase.analysis.summary.MovingAverage;
import edu.stanford.futuredata.macrobase.analysis.summary.MovingAverageTest;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import static org.junit.Assert.*;

public class WindowedOperatorTest {
    @Test
    public void testMovingAverage() throws Exception {
        DataFrame data = MovingAverageTest.getTestDF();
        WindowedOperator<Double> windowedAverageOp = new WindowedOperator<>(
                new MovingAverage("val", 0)
        );
        windowedAverageOp.setSlideLength(10.0);
        windowedAverageOp.setTimeColumn("time");
        windowedAverageOp.setWindowLength(30.0);
        windowedAverageOp.initialize();

        int[] increments = {1, 8, 7, 3, 2, 6, 4, 20, 0, 0, 22, 2, 4};
        double startTime = 0.0;
        for (int miniBatchLength : increments) {
            double endTime = startTime + miniBatchLength;
            double ls = startTime;
            startTime = endTime;

            DataFrame miniBatch = data.filter("time", (double t) -> t >= ls && t < endTime);
            windowedAverageOp.process(miniBatch);
            double curAverage = windowedAverageOp.getResults();

            // [10,20) is the nonzero block, so the operator will first return nonzero results
            // when it sees an element with timestamp = 20 and knows that no more elements with
            // t < 20 will appear
            if (endTime > 20.0 && endTime <= 30.0) {
                assertEquals(0.5, curAverage, 1e-10);
            } else if (endTime > 30.0 && endTime <= 40) {
                assertEquals(1.0 / 3, curAverage, 1e-10);
            } else if (endTime > 50.0) {
                assertEquals(0.0, curAverage, 0.0);
            }
        }

        assertTrue(windowedAverageOp.getBufferSize() > 0);
        windowedAverageOp.flushBuffer();
        assertEquals(windowedAverageOp.getBufferSize(), 0);
    }
}
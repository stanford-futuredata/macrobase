package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.util.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MovingAverageTest {
    @Test
    public void simpleTest() {
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, 3);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        MovingAverage ma = new MovingAverage(conf);
        assertEquals(0, ma.score(TestUtils.createTimeDatum(0, 1)), 0);
        assertEquals(1/3.0, ma.score(TestUtils.createTimeDatum(1, 2)), 1e-5); // Average = 1.5
        assertEquals(1/2.0, ma.score(TestUtils.createTimeDatum(2, 3)), 1e-5); // Average = 2
        assertEquals(1/3.0, ma.score(TestUtils.createTimeDatum(3, 4)), 1e-5); // Average = 3
    }
    
    @Test
    public void weightTest() {
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, 3);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        MovingAverage ma = new MovingAverage(conf);
        assertEquals(0, ma.score(TestUtils.createTimeDatum(0, 1)), 0);
        assertEquals(1/3.0, ma.score(TestUtils.createTimeDatum(1, 2)), 0); // Average = 1.5
        assertEquals(1/3.0, ma.score(TestUtils.createTimeDatum(3, 3)), 0); // This point has twice the weight due to its time, so average = 2.25
    }
}

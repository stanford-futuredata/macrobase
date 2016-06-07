package macrobase.analysis.stats;

import macrobase.analysis.TestUtils;
import macrobase.analysis.stats.ARIMA;
import macrobase.conf.MacroBaseConf;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * These tests are ignored by default so that people running the test suite
 * locally aren't required to have R and rJava installed.
 */
@Ignore public class ARIMATest {
    @Test
    public void constantTest() {
        int windowSize = 10;
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, windowSize);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        for (int i = 0; i < windowSize; i++) {
            a.score(TestUtils.createTimeDatum(i, 1));
        }
        
        assertEquals(0,
            a.score(TestUtils.createTimeDatum(windowSize + 1, 1)),
            1e-5);
        assertEquals(1,
            a.score(TestUtils.createTimeDatum(windowSize + 2, 2)),
            1e-5);
    }
    
    @Test
    public void linearTest() {
        int windowSize = 10;
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, windowSize);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        for (int i = 0; i < windowSize; i++) {
            a.score(TestUtils.createTimeDatum(i, i));
        }
        
        assertEquals(0,
            a.score(TestUtils.createTimeDatum(windowSize, windowSize)),
            1e-5);
        assertEquals(0,
            a.score(TestUtils.createTimeDatum(windowSize + 1, windowSize + 1)),
            1e-5);
        assertEquals(1.0 / (windowSize + 2),
            a.score(TestUtils.createTimeDatum(windowSize + 2, windowSize + 1)),
            1e-5);
    }
    
    @Test
    public void simpleTest() {
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, 6);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        a.score(TestUtils.createTimeDatum(0, 1.5));
        a.score(TestUtils.createTimeDatum(1, 4.5));
        a.score(TestUtils.createTimeDatum(0, 5.0));
        a.score(TestUtils.createTimeDatum(1, 6.5));
        a.score(TestUtils.createTimeDatum(0, 8.5));
        a.score(TestUtils.createTimeDatum(0, 12.0));
        
        assertEquals(0.078,
            a.score(TestUtils.createTimeDatum(1, 13)),
            1e-3);
        assertEquals(0.198,
            a.score(TestUtils.createTimeDatum(1, 13)),
            1e-3);
    }
    
    @Test
    public void windowTest() {
        int windowSize = 5;
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, windowSize);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        for (int i = 0; i < windowSize; i++) {
            a.score(TestUtils.createTimeDatum(i, 1));
        }
        
        // Now model will predict 1 for the next windowSize datum, so score
        // should be (10 - 1) / 1 = 9.
        for (int i = 0; i < windowSize; i++) {
            assertEquals(9,
                a.score(TestUtils.createTimeDatum(windowSize + i, 10)),
                1e-5);
        }

        // Check that after window slide and retrain, model predicts 10.
        assertEquals(0,
            a.score(TestUtils.createTimeDatum(windowSize * 2, 10)),
            1e-5);
    }
}

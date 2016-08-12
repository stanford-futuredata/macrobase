package macrobase.analysis.stats;

import macrobase.analysis.stats.ARIMA;
import macrobase.conf.MacroBaseConf;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * These tests are ignored by default so that people running the test suite
 * locally aren't required to have R and rJava installed.
 */
@Ignore public class ARIMATest {
    public static Datum createTimeDatum(int time, double d) {
        double[] sample = new double[2];
        sample[0] = time;
        sample[1] = d;
        return new Datum(new ArrayList<>(), new ArrayRealVector(sample));
    }
    
    @Test
    public void constantTest() {
        int windowSize = 10;
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, windowSize);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        for (int i = 0; i < windowSize; i++) {
            a.score(createTimeDatum(i, 1));
        }
        
        assertEquals(0,
            a.score(createTimeDatum(windowSize + 1, 1)),
            1e-5);
        assertEquals(1,
            a.score(createTimeDatum(windowSize + 2, 2)),
            1e-5);
    }
    
    @Test
    public void linearTest() {
        int windowSize = 10;
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, windowSize);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        for (int i = 0; i < windowSize; i++) {
            a.score(createTimeDatum(i, i));
        }
        
        assertEquals(0,
            a.score(createTimeDatum(windowSize, windowSize)),
            1e-5);
        assertEquals(0,
            a.score(createTimeDatum(windowSize + 1, windowSize + 1)),
            1e-5);
        assertEquals(1.0 / (windowSize + 2),
            a.score(createTimeDatum(windowSize + 2, windowSize + 1)),
            1e-5);
    }
    
    @Test
    public void simpleTest() {
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, 6);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        a.score(createTimeDatum(0, 1.5));
        a.score(createTimeDatum(1, 4.5));
        a.score(createTimeDatum(0, 5.0));
        a.score(createTimeDatum(1, 6.5));
        a.score(createTimeDatum(0, 8.5));
        a.score(createTimeDatum(0, 12.0));
        
        assertEquals(0.078,
            a.score(createTimeDatum(1, 13)),
            1e-3);
        assertEquals(0.198,
            a.score(createTimeDatum(1, 13)),
            1e-3);
    }
    
    @Test
    public void windowTest() {
        int windowSize = 5;
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TUPLE_WINDOW, windowSize);
        conf.set(MacroBaseConf.TIME_COLUMN, 0);
        ARIMA a = new ARIMA(conf);
        for (int i = 0; i < windowSize; i++) {
            a.score(createTimeDatum(i, 1));
        }
        
        // Now model will predict 1 for the next windowSize datum.
        for (int i = 0; i < windowSize; i++) {
            assertEquals(9,
                a.score(createTimeDatum(windowSize + i, 10)),
                1e-5);
        }

        // Check that after window slide and retrain, model predicts 10.
        assertEquals(0,
            a.score(createTimeDatum(windowSize * 2, 10)),
            1e-5);
    }
}

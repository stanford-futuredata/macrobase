package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class MADTest {
    private static final Logger log = LoggerFactory.getLogger(MADTest.class);

    @Test
    public void simpleTest() {
        MAD m = new MAD(new MacroBaseConf());

        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        m.train(data);
        assertEquals(1.98, m.score(data.get(0)), 1e-5);
        assertEquals(1.98, m.score(data.get(data.size() - 1)), 1e-5);
        assertEquals(0.02, m.score(data.get(50)), 1e-5);
    }

    @Test
    public void zScoreTest() {
        MAD m = new MAD(new MacroBaseConf());

        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        double[] sample = new double[1];
        sample[0] = 20;
        data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));


        m.train(data);
        log.debug("score is {}", m.score(data.get(data.size() - 1)));

        assertEquals(5.0, m.score(data.get(data.size() - 1)), 1e-5);
        assertEquals(5.0 / 1.4826,
                     m.getZScoreEquivalent(m.score(data.get(data.size() - 1))),
                     1e-1);
    }
}

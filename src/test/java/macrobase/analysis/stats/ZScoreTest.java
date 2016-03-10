package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ZScoreTest {
    private static final Logger log = LoggerFactory.getLogger(ZScoreTest.class);

    @Test
    public void simpleTest() {
        ZScore z = new ZScore(new MacroBaseConf());

        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample)));
        }

        z.train(data);
        assertEquals(1.714816, z.score(data.get(0)), 1e-5);
        assertEquals(1.714816, z.score(data.get(data.size() - 1)), 1e-5);
        assertEquals(0.017321, z.score(data.get(50)), 1e-5);
    }

    @Test
    public void zScoreEquivalentTest() {
        ZScore z = new ZScore(new MacroBaseConf());
        assertEquals(1, z.getZScoreEquivalent(1), 0);
        assertEquals(20, z.getZScoreEquivalent(20), 0);
    }
}

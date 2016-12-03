package macrobase.analysis.transform.aggregate;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class BatchWindowAvgTest {
    @Test
    public void testAggregate() throws Exception {
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 10; i ++) {
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(2));
            d.metrics().setEntry(0, i);
            d.metrics().setEntry(1, 1);
            data.add(d);
        }

        BatchWindowAvg windowAvg = new BatchWindowAvg();
        Datum avg = windowAvg.aggregate(data);
        assertEquals(avg.metrics().getEntry(0), 4.5, 1e-5);
        assertEquals(avg.metrics().getEntry(1), 1, 1e-5);

        /* Test datum with time column */
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 0);
        windowAvg = new BatchWindowAvg(conf);
        avg = windowAvg.aggregate(data);
        assert(avg.metrics().getEntry(0) == 0);
        assertEquals(avg.metrics().getEntry(1), 1, 1e-5);
    }
}

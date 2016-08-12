package macrobase.analysis.transform.aggregate;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BatchWindowMaxTest {
    @Test
    public void testAggregate() throws Exception {
        Random r = new Random(0);
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 10; i ++) {
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(3));
            d.metrics().setEntry(0, 1);
            if (i == 0) {
                d.metrics().setEntry(1, r.nextInt());
            } else {
                d.metrics().setEntry(1, Integer.MAX_VALUE);
            }
            d.metrics().setEntry(2, i);
            data.add(d);
        }

        BatchWindowMax windowMax = new BatchWindowMax();
        Datum max = windowMax.aggregate(data);
        assert(max.metrics().getEntry(0) == 1);
        assert(max.metrics().getEntry(1) == Integer.MAX_VALUE);
        assert(max.metrics().getEntry(2) == 9);

        /* Test datum with time column */
        MacroBaseConf conf = new MacroBaseConf().set(MacroBaseConf.TIME_COLUMN, 2);
        windowMax = new BatchWindowMax(conf);
        max = windowMax.aggregate(data);
        assert(max.metrics().getEntry(0) == 1);
        assert(max.metrics().getEntry(1) == Integer.MAX_VALUE);
    }
}

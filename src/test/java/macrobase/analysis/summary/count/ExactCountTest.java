package macrobase.analysis.summary.count;

import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ExactCountTest {
    @Test
    public void testCount() {
        ExactCount ec = new ExactCount();
        HashMap<Integer, Integer> truth = new HashMap<>();

        List<Datum> dws = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < i; ++j) {
                dws.add(new Datum(Lists.newArrayList(i), new ArrayRealVector()));
                truth.compute(i, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        ec.count(dws);

        for (Map.Entry<Integer, Double> cnt : ec.getCounts().entrySet()) {
            assertEquals(truth.get(cnt.getKey()), cnt.getValue(), 1e-10);
        }
    }
}

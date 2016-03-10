package macrobase.analysis.transform;

import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class LinearMetricNormalizerTest {
    @Test
    public void testLinearTransform() {
        Random r = new Random(0);
        List<Datum> data = new ArrayList<>();
        for(int i = 0; i < 10; ++i) {
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(3));
            d.getMetrics().setEntry(0, r.nextInt());
            d.getMetrics().setEntry(1, r.nextInt());
            d.getMetrics().setEntry(2, r.nextInt());
            data.add(d);
        }

        List<Datum> transformed = Lists.newArrayList(new LinearMetricNormalizer(data.iterator()));
        for(Datum d : transformed) {
            for(int dim = 0; dim < 3; ++dim) {
                Double val = d.getMetrics().getEntry(dim);
                assertTrue(val >= 0);
                assertTrue(val <= 1);
            }
        }
    }

}

package macrobase.analysis.transform;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class LinearMetricNormalizerTest {
    @Test
    public void testLinearTransform() throws Exception {
        Random r = new Random(0);
        List<Datum> data = new ArrayList<>();
        for(int i = 0; i < 10; ++i) {
            Datum d = new Datum(new ArrayList<>(), new ArrayRealVector(3));
            d.metrics().setEntry(0, r.nextInt());
            d.metrics().setEntry(1, r.nextInt());
            d.metrics().setEntry(2, r.nextInt());
            data.add(d);
        }

        LinearMetricNormalizer lmn = new LinearMetricNormalizer();
        lmn.consume(data);
        List<Datum> transformed = lmn.getStream().drain();
        for(Datum d : transformed) {
            for(int dim = 0; dim < 3; ++dim) {
                Double val = d.metrics().getEntry(dim);
                assertTrue(val >= 0);
                assertTrue(val <= 1);
            }
        }
    }

}

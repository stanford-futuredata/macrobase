package macrobase.analysis.transform;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LinearMetricNormalizer extends BatchTransform {
    private static final Logger log = LoggerFactory.getLogger(LinearMetricNormalizer.class);

    public LinearMetricNormalizer(Iterator<Datum> inputIterator) {
        super(inputIterator);
    }

    protected List<Datum> transform(List<Datum> data) {

        List<Datum> ret = new ArrayList<>(data.size());
        int dimensions = data.get(0).getMetrics().getDimension();

        RealVector metricWiseMinVec = new ArrayRealVector(dimensions);
        RealVector metricWiseMaxVec = new ArrayRealVector(dimensions);

        for (Datum d : data) {
            for (int i = 0; i < dimensions; i++) {
                double val = d.getMetrics().getEntry(i);

                if (metricWiseMinVec.getEntry(i) > val) {
                    metricWiseMinVec.setEntry(i, val);
                }

                if (metricWiseMaxVec.getEntry(i) < val) {
                    metricWiseMaxVec.setEntry(i, val);
                }
            }
        }

        for (Datum d : data) {
            RealVector metrics = d.getMetrics().copy();
            for (int dim = 0; dim < metrics.getDimension(); ++dim) {
                double dimMin = metricWiseMinVec.getEntry(dim);
                double dimMax = metricWiseMaxVec.getEntry(dim);

                if (dimMax - dimMin == 0) {
                    metrics.setEntry(dim, 0);
                    continue;
                }

                double cur = metrics.getEntry(dim);
                metrics.setEntry(dim, (cur - dimMin) / (dimMax - dimMin));
            }
            ret.add(new Datum(d, metrics));
        }

        return ret;
    }
}

package macrobase.ingest.transform;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZeroToOneLinearTransformation extends DataTransformation {

    private static final Logger log = LoggerFactory.getLogger(ZeroToOneLinearTransformation.class);

    @Override
    public void transform(List<Datum> data) {

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
            // ebeDivide returns a copy; avoid a copy at the expense of ugly code
            RealVector metrics = d.getMetrics();
            for (int dim = 0; dim < metrics.getDimension(); ++dim) {
                double dimMin = metricWiseMinVec.getEntry(dim);
                double dimMax = metricWiseMaxVec.getEntry(dim);

                if (dimMax - dimMin == 0) {
                    log.warn("No difference between min and max in dimension {}!", dim);
                    metrics.setEntry(dim, 0);
                    continue;
                }

                double cur = metrics.getEntry(dim);
                metrics.setEntry(dim, (cur - dimMin) / (dimMax - dimMin));
            }
        }
    }
}

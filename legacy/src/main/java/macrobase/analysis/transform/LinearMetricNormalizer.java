package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.List;

public class LinearMetricNormalizer extends FeatureTransform {
    private MBStream<Datum> output = new MBStream<>();

    @Override
    public void consume(List<Datum> data) {
        int dimensions = data.get(0).metrics().getDimension();

        RealVector metricWiseMinVec = new ArrayRealVector(dimensions);
        RealVector metricWiseMaxVec = new ArrayRealVector(dimensions);

        for (Datum d : data) {
            for (int i = 0; i < dimensions; i++) {
                double val = d.metrics().getEntry(i);

                if (metricWiseMinVec.getEntry(i) > val) {
                    metricWiseMinVec.setEntry(i, val);
                }

                if (metricWiseMaxVec.getEntry(i) < val) {
                    metricWiseMaxVec.setEntry(i, val);
                }
            }
        }

        for (Datum d : data) {
            RealVector metrics = d.metrics().copy();
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
            output.add(new Datum(d, metrics));
        }
    }

    @Override
    public void initialize() throws Exception {

    }


    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }
}

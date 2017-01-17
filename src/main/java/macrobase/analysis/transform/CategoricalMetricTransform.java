package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.HashMap;
import java.util.List;

public class CategoricalMetricTransform extends FeatureTransform {
    private MBStream<Datum> output = new MBStream<>();

    @Override
    public void consume(List<Datum> data) {
        
        int numCategoricalMetrics = data.get(0).getCategoricalMetrics().size();
        HashMap<Integer, HashMap<Integer, Integer>> whichCategoricalMetric2Value2Count = new HashMap<Integer, HashMap<Integer, Integer>>();
        for (int i = 0; i < numCategoricalMetrics; i++) {
            HashMap<Integer, Integer> value2Count = new HashMap<Integer, Integer>();
            for (Datum d : data) {
                Integer value = d.getCategoricalMetrics().get(i);
                if (value2Count.containsKey(value)) {
                    value2Count.put(value, value2Count.get(value) + 1);
                } else {
                    value2Count.put(value, 1);
                }
            }
            whichCategoricalMetric2Value2Count.put(i, value2Count);
        }
        
        for (Datum d : data) {
            RealVector metrics = new ArrayRealVector(numCategoricalMetrics);
            for (int i = 0; i < numCategoricalMetrics; i++) {
                Integer value = d.getCategoricalMetrics().get(i);
                metrics.setEntry(i, whichCategoricalMetric2Value2Count.get(i).get(value));
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

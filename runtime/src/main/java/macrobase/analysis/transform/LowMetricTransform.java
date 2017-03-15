package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

/*
 Takes the reciprocal of the specified metrics
 */
public class LowMetricTransform extends FeatureTransform {
    MBStream<Datum> output = new MBStream<>();

    final List<Integer> toTransform;

    public LowMetricTransform(MacroBaseConf conf) throws ConfigurationException {
        toTransform = new ArrayList<>();
        List<String> transformNames = conf.getStringList(MacroBaseConf.LOW_METRIC_TRANSFORM);
        List<String> metrics = conf.getStringList(MacroBaseConf.METRICS);

        for(String name : transformNames) {
            toTransform.add(metrics.indexOf(name));
        }
    }

    public LowMetricTransform(List<Integer> indexesToTransform) {
        toTransform = indexesToTransform;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        for(Datum d : records) {
            for(int idx : toTransform) {
                double prevVal = d.metrics().getEntry(idx);
                d.metrics().setEntry(idx, Math.pow(Math.max(prevVal, 0.1), -1));
            }

            output.add(d);
        }
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }
}

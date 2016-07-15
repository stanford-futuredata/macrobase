package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.RealVector;

import java.util.List;

public class ProjectMetric extends FeatureTransform{
    private RealVector metricVector;
    private RealVector transformedVector;
    private int targetDim;

    private final MBStream<Datum> output = new MBStream<>();


    public ProjectMetric(int targetDim) {
        this.targetDim = targetDim;
    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        for (Datum d: records){
            metricVector = d.getMetrics();
            transformedVector = metricVector.getSubVector(targetDim,1);
            output.add(new Datum(d,transformedVector));
        }
    }

    @Override
    public MBStream<Datum> getStream() throws Exception  {
        return output;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }
}

package macrobase.analysis.stats;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import java.util.List;

public class FFT extends FeatureTransform {
    private RealVector metricVector;
    private RealVector paddedInput;
    private RealVector transformedMetricVector;
    private FastFourierTransformer transformer;
    private Complex[] FFTOutput;
    private int nextPowTwo;

    private final MBStream<Datum> output = new MBStream<>();


    public FFT(MacroBaseConf conf) {
        //no config options or anything for now.
    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        for (Datum d: records){
            metricVector = d.metrics();
            // TODO: look for decent FFT implementation that doesn't need pwr of 2
            nextPowTwo = Math.max(2,2*Integer.highestOneBit(metricVector.getDimension()-1));
            paddedInput = metricVector.append(new ArrayRealVector(nextPowTwo - metricVector.getDimension()));

            transformer = new FastFourierTransformer(DftNormalization.STANDARD);
            FFTOutput = transformer.transform(paddedInput.toArray(), TransformType.FORWARD);
            transformedMetricVector = new ArrayRealVector();
            for (Complex c: FFTOutput){
                transformedMetricVector = transformedMetricVector.append(c.getReal());
                transformedMetricVector = transformedMetricVector.append(c.getImaginary());
            }
            output.add(new Datum(d, transformedMetricVector));
        }
    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }


    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }

}

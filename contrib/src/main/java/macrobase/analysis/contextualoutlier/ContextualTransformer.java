package macrobase.analysis.contextualoutlier;


import macrobase.MacroBase;
import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public class ContextualTransformer extends MBOperator<Datum, ContextualDatum> {
    private final MacroBaseConf conf;
    private final MBStream<ContextualDatum> output = new MBStream<>();

    public ContextualTransformer(MacroBaseConf conf) {
        this.conf = conf;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        List<ContextualDatum> converted = new ArrayList<>(records.size());
        for(Datum d : records) {
            converted.add(new ContextualDatum(d, conf));
        }

        output.add(converted);
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<ContextualDatum> getStream() throws Exception {
        return output;
    }
}

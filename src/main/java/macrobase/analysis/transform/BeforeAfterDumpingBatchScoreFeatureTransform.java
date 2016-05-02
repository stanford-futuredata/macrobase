package macrobase.analysis.transform;

import macrobase.analysis.pipeline.operator.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.JsonUtils;
import macrobase.diagnostics.MetricsAndMetrics;

import java.util.ArrayList;
import java.util.List;

public class BeforeAfterDumpingBatchScoreFeatureTransform implements FeatureTransform {

    private final String dumpFilename;
    private FeatureTransform underlyingTransform;
    private final MBStream<Datum> output = new MBStream<>();

    public BeforeAfterDumpingBatchScoreFeatureTransform(MacroBaseConf conf, FeatureTransform transform) throws ConfigurationException {
        this.dumpFilename = conf.getString(MacroBaseConf.SCORED_DATA_FILE, MacroBaseDefaults.SCORED_DATA_FILE);
        this.underlyingTransform = transform;
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        List<Datum> initalRecords = records;
        underlyingTransform.consume(records);
        List<Datum> transferredRecords = underlyingTransform.getStream().drain();

        if (this.dumpFilename != null) {
            List<MetricsAndMetrics> beforeAndAfter = new ArrayList<>(initalRecords.size());
            for (int i = 0; i < initalRecords.size(); i++) {
                beforeAndAfter.add(new MetricsAndMetrics(initalRecords.get(i).getMetrics(), transferredRecords.get(i).getMetrics()));
            }
            JsonUtils.tryToDumpAsJson(beforeAndAfter, this.dumpFilename);
        }
        output.add(transferredRecords);
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }
}

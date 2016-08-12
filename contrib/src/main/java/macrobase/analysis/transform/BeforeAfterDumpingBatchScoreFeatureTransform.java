package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.JsonUtils;
import macrobase.diagnostics.MetricsAndMetrics;
import macrobase.diagnostics.ScoreDumper;

import java.util.ArrayList;
import java.util.List;

public class BeforeAfterDumpingBatchScoreFeatureTransform extends FeatureTransform {

    public static final String SCORED_DATA_FILE = null;
    private final String dumpFilename;
    private FeatureTransform underlyingTransform;
    private final MBStream<Datum> output = new MBStream<>();

    public BeforeAfterDumpingBatchScoreFeatureTransform(MacroBaseConf conf, FeatureTransform transform) throws ConfigurationException {
        this.dumpFilename = conf.getString(ScoreDumper.SCORED_DATA_FILE, SCORED_DATA_FILE);
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
            for (int i = 0; i < transferredRecords.size(); i++) {
                beforeAndAfter.add(new MetricsAndMetrics(initalRecords.get(i).metrics(), transferredRecords.get(i).metrics()));
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

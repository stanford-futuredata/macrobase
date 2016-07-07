package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.ContributingDatum;
import macrobase.analysis.stats.MinCovDet;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import javax.validation.constraints.Min;
import java.util.List;

public class BatchScoreFeatureTransform extends FeatureTransform {
    public BatchTrainScore batchTrainScore;
    protected MacroBaseConf conf;

    private boolean requiresTraining = true;
    protected final MBStream<Datum> output = new MBStream<>();

    public BatchScoreFeatureTransform(MacroBaseConf conf, MacroBaseConf.TransformType transformType)
            throws ConfigurationException {
        this.batchTrainScore = conf.constructTransform(transformType);
        this.conf = conf;
    }

    public BatchScoreFeatureTransform(BatchTrainScore batchTrainScore, boolean requiresTraining) {
        this.batchTrainScore = batchTrainScore;
        this.requiresTraining = requiresTraining;
    }
    
    @Override
    public void initialize() {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        if (requiresTraining)
            batchTrainScore.train(records);
        if (conf == null || (conf != null && conf.getTransformType() != MacroBaseConf.TransformType.MCD)) {
            for (Datum d : records) {
                output.add(new Datum(d, batchTrainScore.score(d)));
            }
        } else {
            for (Datum d : records) {
                ContributingDatum cd = new ContributingDatum(d,
                                                             batchTrainScore.score(d),
                                                             ((MinCovDet) batchTrainScore).computeContribution(d.getMetrics()));
                output.add(cd);
            }
        }
    }

    @Override
    public void shutdown() {

    }

    public BatchTrainScore getBatchTrainScore() {
        return batchTrainScore;
    }

    @Override
    public MBStream<Datum> getStream() {
        return output;
    }
}
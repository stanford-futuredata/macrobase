package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public class BatchScoreFeatureTransform extends FeatureTransform {
    protected BatchTrainScore batchTrainScore;
    protected MacroBaseConf conf;

    private boolean requiresTraining = true;
    protected final MBStream<Datum> output = new MBStream<>();

    public BatchScoreFeatureTransform(MacroBaseConf conf)
            throws ConfigurationException {
        this.batchTrainScore = conf.constructTransform();
        this.conf = conf;
    }

    public BatchScoreFeatureTransform(MacroBaseConf conf, BatchTrainScore bts)
            throws ConfigurationException {
        this.batchTrainScore = bts;
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
    public void consume(List<Datum> records) {
        if(requiresTraining) {
            batchTrainScore.train(records);
        }

        for(Datum d : records) {
            output.add(new Datum(d, batchTrainScore.score(d)));
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
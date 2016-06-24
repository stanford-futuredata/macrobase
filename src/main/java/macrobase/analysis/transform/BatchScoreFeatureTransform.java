package macrobase.analysis.transform;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class BatchScoreFeatureTransform extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(BatchScoreFeatureTransform.class);
    protected BatchTrainScore batchTrainScore;
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
    public void consume(List<Datum> records) {
        Stopwatch sw = Stopwatch.createStarted();
        if (requiresTraining)
            batchTrainScore.train(records);
        final long trainMs = sw.elapsed(TimeUnit.MILLISECONDS);
        for (Datum d : records) {
            output.add(new Datum(d, batchTrainScore.score(d)));
        }
        final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS);
        final long scoreMs = totalMs - trainMs;
        log.info("FeatureTransform took {}ms ({} tuples/sec)",
                totalMs, 1000 * records.size() / (double) totalMs);
        log.info("training took {}ms ({} tuples/sec)",
                trainMs, 1000 * records.size() / (double) trainMs);
        log.info("scoring took {}ms ({} tuples/sec)",
                scoreMs, 1000 * records.size() / (double) scoreMs);
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
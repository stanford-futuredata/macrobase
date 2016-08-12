package macrobase.diagnostic.tasks;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostic.tasks.TrueDensityISECalculator;
import macrobase.ingest.DataIngester;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class HeldOutDataLogLikelihoodCalc extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(TrueDensityISECalculator.class);
    public static final String HOLD_OUT_PERCENTAGE = "macrobase.diagnostic.holdOutPercentage";


    @Override
    public List<AnalysisResult> run() throws Exception {
        DataIngester ingester = conf.constructIngester();

        List<Datum> data = ingester.getStream().drain();

        long startMs = System.currentTimeMillis();
        double keepRatio = conf.getDouble(HOLD_OUT_PERCENTAGE, 0.1);

        List<Datum> trainingData = new ArrayList<>((int) (data.size() * (1 - keepRatio)));
        List<Datum> testData = new ArrayList<>((int) (data.size() * keepRatio));

        Random rand = conf.getRandom();
        log.debug("nextDouble() {}", rand.nextDouble());
        for (Datum d : data) {
            if (rand.nextDouble() < keepRatio) {
                testData.add(d);
            } else {
                trainingData.add(d);
            }
        }
        log.debug("training points = {}", trainingData.size());
        log.debug("test points = {}", testData.size());

        long loadEndMs = System.currentTimeMillis();

        BatchScoreFeatureTransform batchTransform = new BatchScoreFeatureTransform(conf);

        batchTransform.consume(trainingData);

        BatchTrainScore batchTrainScore = batchTransform.getBatchTrainScore();

        double overallScore = 0;
        for (Datum d : testData) {
            overallScore += batchTrainScore.score(d);
        }
        overallScore /= testData.size();

        log.debug("Per point score: {}", overallScore);
        final long endMs = System.currentTimeMillis();
        final long loadMs = loadEndMs - startMs;
        final long totalMs = endMs - loadEndMs;

        return Arrays.asList(new AnalysisResult(0,
                0,
                loadMs,
                totalMs,
                0,
                new ArrayList<>()));
    }
}

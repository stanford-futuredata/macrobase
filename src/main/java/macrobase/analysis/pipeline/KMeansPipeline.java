package macrobase.analysis.pipeline;

import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.mixture.VarGMM;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.util.TrainTestSpliter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class KMeansPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(KMeansPipeline.class);

    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        long startMs = System.currentTimeMillis();
        DataIngester ingester = conf.constructIngester();

        List<Datum> data = ingester.getStream().drain();
        long loadEndMs = System.currentTimeMillis();

        Random rand = conf.getRandom();
        VarGMM vargmm = new VarGMM(conf);
        List<Datum> globalTrainData;
        TrainTestSpliter trainTestSpliter = new TrainTestSpliter(data, 0.9, conf.getRandom());
        globalTrainData = trainTestSpliter.getTrainData();
        vargmm.kmeansInitialize(globalTrainData);

        vargmm.sviTrainToConvergeAndMonitor(globalTrainData, trainTestSpliter.getTestData());

        final long trainEndMs = System.currentTimeMillis();
        vargmm.meanLogLike(trainTestSpliter.getTestData());

        final long endMs = System.currentTimeMillis();
        final long loadMs = loadEndMs - startMs;
        final long trainMs = trainEndMs - loadEndMs;
        final long testMs = endMs - trainEndMs;
        final long execMs = endMs - loadEndMs;

        log.info("took {}ms ({} tuples/sec)",
                execMs,
                (data.size()) / (double) execMs * 1000);

        log.info("training took {}ms ({} tuples/sec)",
                trainMs,
                (trainTestSpliter.getTrainData().size()) / (double) trainMs * 1000);

        log.info("scoring took {}ms ({} tuples/sec)",
                testMs,
                (trainTestSpliter.getTestData().size()) / (double) testMs * 1000);

        return Arrays.asList(new AnalysisResult(0,
                0,
                loadMs,
                execMs,
                0,
                new ArrayList<ItemsetResult>()));
    }

}

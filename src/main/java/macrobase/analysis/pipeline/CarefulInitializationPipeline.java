package macrobase.analysis.pipeline;

import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.mixture.VarGMM;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class CarefulInitializationPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(CarefulInitializationPipeline.class);

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

        final int numTransforms = 10;
        List<VarGMM> varGMMs = new ArrayList<>(numTransforms);
        for (int i = 0; i < numTransforms; i++) {
            varGMMs.add(new VarGMM(conf));
        }

        Random rand = conf.getRandom();
        List<List<Datum>> dataList = new ArrayList<>(numTransforms);
        for (int i = 0; i < 1 + numTransforms; i++) {
            // 1 + N lists, N for training, 1 for testing.
            dataList.add(new ArrayList<>());
        }
        List<Datum> globalTrainData = new ArrayList<>();
        List<Datum> globalTestData = new ArrayList<>();

        for (Datum d : data) {
            int index = rand.nextInt(1 + numTransforms);
            dataList.get(index).add(d);
            if (index < numTransforms) {
                globalTrainData.add(d);
            } else {
                globalTestData.add(d);
            }
        }

        int bestIndex = -1;
        double bestScore = -Double.MAX_VALUE;

        for (int i = 0; i < numTransforms; i++) {
            varGMMs.get(i).initialize(dataList.get(i));
            //VariationalInference.trainTestMeanField(batchVarGMMs.get(i), );
            log.debug("sublist {} size = {}", i, dataList.get(i).size());
            varGMMs.get(i).sviLoop(dataList.get(i));
            double meanLogLike = varGMMs.get(i).meanLogLike(dataList.get(numTransforms));
            log.debug("test score: {}", meanLogLike);
            if (meanLogLike > bestScore) {
                bestIndex = i;
            }
        }

        VarGMM varGMM = varGMMs.get(bestIndex);

        varGMM.sviTrainToConvergeAndMonitor(globalTrainData, globalTestData);

        final long trainEndMs = System.currentTimeMillis();
        varGMM.meanLogLike(globalTestData);

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
                (data.size()) / (double) trainMs * 1000);

        log.info("scoring took {}ms ({} tuples/sec)",
                testMs,
                (data.size()) / (double) testMs * 1000);

        return Arrays.asList(new AnalysisResult(0,
                0,
                loadMs,
                execMs,
                0,
                new ArrayList<ItemsetResult>()));
    }

}

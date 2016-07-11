package macrobase.analysis.pipeline;

import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.mixture.BatchVarGMM;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
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
        List<BatchVarGMM> batchVarGMMs = new ArrayList<>(numTransforms);
        for (int i = 0; i < numTransforms; i++) {
            batchVarGMMs.add((BatchVarGMM) conf.constructTransform(conf.getTransformType()));
        }

        Random rand = conf.getRandom();
        List<List<Datum>> dataList = new ArrayList<>(numTransforms);
        for (int i = 0; i < 1 + numTransforms; i++) {
            // 1 + N lists, N for training, 1 for testing.
            dataList.add(new ArrayList<>());
        }
        for (Datum d : data) {
            int index = rand.nextInt(1 + numTransforms);
            dataList.get(index).add(d);
        }

        for (int i = 0; i < numTransforms; i++) {
            batchVarGMMs.get(i).initialize(dataList.get(i));
            //VariationalInference.trainTestMeanField(batchVarGMMs.get(i), );
            log.debug("sublist {} size = {}", i, dataList.get(i).size());
        }

        OutlierClassifier outlierClassifier = new BatchingPercentileClassifier(conf);
        //outlierClassifier.consume(dumpingTransform.getStream().drain());
        // FIXME:
        outlierClassifier.consume(data);

        BatchSummarizer summarizer = new BatchSummarizer(conf);
        summarizer.consume(outlierClassifier.getStream().drain());

        Summary result = summarizer.summarize().getStream().drain().get(0);

        final long endMs = System.currentTimeMillis();
        final long loadMs = loadEndMs - startMs;
        final long totalMs = endMs - loadEndMs;
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - result.getCreationTimeMs();

        log.info("took {}ms ({} tuples/sec)",
                totalMs,
                (result.getNumInliers() + result.getNumOutliers()) / (double) totalMs * 1000);

        return Arrays.asList(new AnalysisResult(result.getNumOutliers(),
                result.getNumInliers(),
                loadMs,
                executeMs,
                summarizeMs,
                result.getItemsets()));
    }

}

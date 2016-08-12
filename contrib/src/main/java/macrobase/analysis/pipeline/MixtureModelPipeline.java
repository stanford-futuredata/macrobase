package macrobase.analysis.pipeline;

import macrobase.analysis.classify.MixtureGroupClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.mixture.GMMConf;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.BatchMixtureCoeffTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.GridDumpingBatchScoreTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MixtureModelPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(MixtureModelPipeline.class);

    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        long startMs = System.currentTimeMillis();
        DataIngester ingester = conf.constructIngester();

        List<Datum> data = ingester.getStream().drain();
        long loadEndMs = System.currentTimeMillis();

        BatchMixtureCoeffTransform mixtureProbabilityTransform = new BatchMixtureCoeffTransform(conf);
        FeatureTransform gridDumpingTransform = new GridDumpingBatchScoreTransform(conf, mixtureProbabilityTransform);
        gridDumpingTransform.initialize();
        gridDumpingTransform.consume(data);

        List<Datum> transferedData = gridDumpingTransform.getStream().drain();


        if (conf.isSet(GMMConf.TARGET_GROUP)) {

            OutlierClassifier outlierClassifier = new MixtureGroupClassifier(conf, mixtureProbabilityTransform.getMixtureModel());
            outlierClassifier.consume(transferedData);

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
        } else {
            List<AnalysisResult> results = new ArrayList<>();
            for (int i = 0; i < transferedData.get(0).metrics().getDimension(); i++) {
                OutlierClassifier outlierClassifier = new MixtureGroupClassifier(conf, mixtureProbabilityTransform.getMixtureModel(), i);
                outlierClassifier.consume(transferedData);

                BatchSummarizer summarizer = new BatchSummarizer(conf);
                summarizer.consume(outlierClassifier.getStream().drain());

                Summary summary = summarizer.summarize().getStream().drain().get(0);
                results.add(new AnalysisResult(
                        summary.getNumOutliers(),
                        summary.getNumInliers(),
                        0,
                        0,
                        0,
                        summary.getItemsets()));
            }
            return results;
        }
    }
}

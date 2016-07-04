package macrobase.bench;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PrecisionRecallPipelineTest extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(PrecisionRecallPipelineTest.class);

    @Override
    public PrecisionRecallPipelineTest initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    private Summary execute(List<Datum> input, MacroBaseConf conf) throws Exception {
        FeatureTransform ft = new BatchScoreFeatureTransform(conf, MacroBaseConf.TransformType.MAD);
        ft.consume(input);

        OutlierClassifier oc = new BatchingPercentileClassifier(conf);
        oc.consume(ft.getStream().drain());

        Summarizer bs = new BatchSummarizer(conf);
        bs.consume(oc.getStream().drain());
        Summary result = bs.summarize().getStream().drain().get(0);

        return result;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        conf.set(MacroBaseConf.MIN_SUPPORT, 0.001);
        conf.set(MacroBaseConf.MIN_OI_RATIO, 3);
        runOnConf("OI3");

        conf.set(MacroBaseConf.MIN_OI_RATIO, 0);
        runOnConf("OI0");

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }

    private void runOnConf(String prefix) throws Exception {
        int numPoints = 10000000;
        double percentageOutliers = 0.01;

        for(Double labelNoise : Lists.newArrayList(0d, 0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5)) {
            for (Integer outlierClusters : Lists.newArrayList(10, 100, 1000, 10000)) {
                trial(prefix+"labelNoise", numPoints, percentageOutliers, outlierClusters, labelNoise, 0);
            }
        }

        for(Double pointNoise : Lists.newArrayList(0d, 0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5)) {
            for (Integer outlierClusters : Lists.newArrayList(10, 100, 1000, 10000)) {
                trial(prefix+"pointNoise", numPoints, percentageOutliers, outlierClusters, 0, pointNoise);
            }
        }
    }

    private void trial(String name, int numPoints, double percentageOutliers, int outlierClusters, double labelNoise, double pointNoise) throws Exception {
        RandomGenerator g = new MersenneTwister(0);

        NormalDistribution inliers = new NormalDistribution(g, 10, 10);
        NormalDistribution outliers = new NormalDistribution(g, 70, 10);
        UniformRealDistribution extraneous = new UniformRealDistribution(g, 0, 80);

        conf.getEncoder().copy(new DatumEncoder());
        conf.getEncoder().recordAttributeName(0, "ATTR");

        HashSet<String> trueOutlierAttributes = new HashSet<>();

        for(Integer i = 0; i < outlierClusters; ++i) {
            trueOutlierAttributes.add(i.toString());
        }

        List<Datum> data = new ArrayList<>();

        Random r = new Random();

        for(Integer i = 0; i < numPoints; ++i) {
            double val;
            Integer attr = null;

            boolean isOutlier = ((((float)i / numPoints) < percentageOutliers));

            if(r.nextDouble() < pointNoise) {
                val = extraneous.sample();
            } else if(isOutlier) {
                val = outliers.sample();
            } else {
                val = inliers.sample();
            }

            boolean outlierLabel = isOutlier;

            if(r.nextDouble() < labelNoise) {
                outlierLabel = !outlierLabel;
            }

            if(attr == null) {
                if (outlierLabel) {
                    attr = conf.getEncoder().getIntegerEncoding(0, String.valueOf(i % outlierClusters));
                } else {
                    attr = conf.getEncoder().getIntegerEncoding(0, String.valueOf(
                            (r.nextInt((int)(outlierClusters*(1-percentageOutliers)/(percentageOutliers)))) + outlierClusters));
                }
            }

            data.add(new Datum(Lists.newArrayList(attr), val));
        }

        FeatureTransform normalizer = new LinearMetricNormalizer();
        normalizer.consume(data);
        data = normalizer.getStream().drain();

        System.gc();
        Stopwatch sw = Stopwatch.createStarted();
        Summary s = execute(data, conf);
        sw.stop();

        List<String> foundOutlierAttributes = s.getItemsets().stream().map(i -> i.getItems().get(0).getValue()).collect(
                Collectors.toList());

        double precision = foundOutlierAttributes.size() > 0 ? foundOutlierAttributes.stream().filter(a -> trueOutlierAttributes.contains(a)).count()/((float)(foundOutlierAttributes.size())) : 0;
        double recall = foundOutlierAttributes.stream().filter(a -> trueOutlierAttributes.contains(a)).count()/((float)(trueOutlierAttributes.size()));

        log.error("TRIAL: {}, CLUSTERS: {}, PRECISION: {}, RECALL: {}, NUM_SUMMARIES: {}, TIME: {}, LABELNOISE: {}, POINTNOISE: {}",
                  name, outlierClusters, precision, recall, s.getItemsets().size(), sw.elapsed(
                TimeUnit.MILLISECONDS), labelNoise, pointNoise);
    }
}
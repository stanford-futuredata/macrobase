package macrobase.bench;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RatioSupportSweepPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(RatioSupportSweepPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        DataIngester ingester = conf.constructIngester();
        FeatureTransform normalizer = new LinearMetricNormalizer();
        normalizer.consume(ingester.getStream().drain());
        List<Datum> data = normalizer.getStream().drain();
        System.gc();

        FeatureTransform ft = new BatchScoreFeatureTransform(conf, conf.getTransformType());
        ft.consume(data);

        OutlierClassifier oc = new BatchingPercentileClassifier(conf);

        oc.consume(ft.getStream().drain());

        List<OutlierClassificationResult> ocr = oc.getStream().drain();

        List<Datum> outliers = new ArrayList<>();
        List<Datum> inliers = new ArrayList<>();

        for(OutlierClassificationResult r : ocr) {
            if(r.isOutlier()) {
                outliers.add(r.getDatum());
            } else {
                inliers.add(r.getDatum());
            }
        }

        int iterations = 5;

        double[] supports = {1e-5, 1e-4, 1e-3, 1e-2, 1e-1, .25, .5};
        double[] ratios = {1e-2, 1e-1, 1, 2, 3, 5, 10};

        final double defaultsupport = 0.001;
        final double defaultratio = 3;

        for(int i = 0; i < iterations; ++i) {
            for (double support : supports) {
                Stopwatch sw = Stopwatch.createStarted();
                FPGrowthEmerging fpg = new FPGrowthEmerging(true);
                List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(inliers,
                                                                                outliers,
                                                                                support,
                                                                                defaultratio,
                                                                                conf.getEncoder());
                sw.stop();
                log.info("SWEEPSUPPORT: {}, {}, {} itemsets", support, sw.elapsed(TimeUnit.MICROSECONDS), isr.size());
                sw.reset();
            }

            for (double ratio : ratios) {
                Stopwatch sw = Stopwatch.createStarted();
                FPGrowthEmerging fpg = new FPGrowthEmerging(true);
                List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(inliers,
                                                                                outliers,
                                                                                defaultsupport,
                                                                                ratio,
                                                                                conf.getEncoder());
                sw.stop();
                log.info("SWEEPRATIO: {}, {}, {} itemsets", ratio, sw.elapsed(TimeUnit.MICROSECONDS), isr.size());
                sw.reset();
            }
        }

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}
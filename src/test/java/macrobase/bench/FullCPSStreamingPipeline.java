package macrobase.bench;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.itemset.Apriori;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.bench.compare.itemset.CPSTreeEmergingItemsets;
import macrobase.bench.compare.summary.CubeCompare;
import macrobase.bench.compare.summary.DataXRayCompare;
import macrobase.bench.compare.summary.DecisionTreeCompare;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.ingest.result.ColumnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FullCPSStreamingPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(SummaryComparePipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        FeatureTransform normalizer = new LinearMetricNormalizer();
        normalizer.consume(ingester.getStream().drain());
        List<Datum> data = normalizer.getStream().drain();
        System.gc();

        FeatureTransform ft = new BatchScoreFeatureTransform(conf, conf.getTransformType());
        ft.consume(data);

        OutlierClassifier oc = new BatchingPercentileClassifier(conf);

        if (conf.getBoolean(MacroBaseConf.CLASSIFIER_DUMP)) {
            String queryName = conf.getString(MacroBaseConf.QUERY_NAME);
            oc = new DumpClassifier(conf, oc, queryName);
        }

        oc.consume(ft.getStream().drain());


        List<OutlierClassificationResult> scored = oc.getStream().drain();

        List<Set<Integer>> in_txns = new ArrayList<>();
        List<Set<Integer>> out_txns = new ArrayList<>();

        List<OutlierClassificationResult> inliers = new ArrayList<>();
        List<OutlierClassificationResult> outliers = new ArrayList<>();

        List<Datum> inlier_data = new ArrayList<>();
        List<Datum> outlier_data = new ArrayList<>();

        final int ITERATIONS = 5;
        final int PERIOD = 100000;


        for(OutlierClassificationResult d : scored) {
            if(d.isOutlier()) {
                in_txns.add(Sets.newHashSet(d.getDatum().getAttributes()));
                inliers.add(d);
                inlier_data.add(d.getDatum());
            } else {
                out_txns.add(Sets.newHashSet(d.getDatum().getAttributes()));
                outliers.add(d);
                outlier_data.add(d.getDatum());
            }
        }

        Double summaryPeriod = conf.getDouble(MacroBaseConf.SUMMARY_UPDATE_PERIOD,
                                              MacroBaseDefaults.SUMMARY_UPDATE_PERIOD);
        Double decayRate = conf.getDouble(MacroBaseConf.DECAY_RATE, MacroBaseDefaults.DECAY_RATE);
        Integer outlierItemSummarySize = conf.getInt(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE,
                                                     MacroBaseDefaults.OUTLIER_ITEM_SUMMARY_SIZE);
        Integer inlierItemSummarySize = conf.getInt(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE,
                                                    MacroBaseDefaults.INLIER_ITEM_SUMMARY_SIZE);

        Double minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
        Double minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);

        for(int i = 0; i < ITERATIONS; ++i) {
            System.gc();
            sw.reset();
            sw.start();

            List<String> attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);


            CPSTreeEmergingItemsets testSummarizer =
                    new CPSTreeEmergingItemsets(inlierItemSummarySize,
                                                outlierItemSummarySize,
                                                minSupport,
                                                minOIRatio,
                                                decayRate,
                                                attributes.size(),
                                                true);

            for(int pt = 0; pt < scored.size(); ++pt) {
                OutlierClassificationResult p = scored.get(pt);
                if(p.isOutlier()){
                    testSummarizer.markOutlier(p.getDatum());
                } else {
                    testSummarizer.markInlier(p.getDatum());
                }

                if(pt % PERIOD == 0) {
                    testSummarizer.markPeriod();
                }
            }

            List<ItemsetResult> isr = testSummarizer.getItemsets(conf.getEncoder());
            sw.stop();
            log.info("EXACTCPS: {} {}", sw.elapsed(TimeUnit.MICROSECONDS), isr.size());
            for(ItemsetResult is: isr) {
                log.info("EXACTCPS: {} {} {} {}", is.getItems().size(), is.getSupport(), is.getRatioToInliers(), is.getNumRecords());
                for(ColumnValue cv : is.getItems()) {
                    log.info("{} {}", cv.getColumn(), cv.getValue());
                }

                log.info("EXACT_ITEMS: {}", is.getItems().stream().map(a -> a.getColumn()+":"+a.getValue()).collect(
                        Collectors.joining(",")));
                log.info("");
            }
            sw.reset();


            System.gc();
            sw.reset();
            sw.start();


            ExponentiallyDecayingEmergingItemsets testSummarizer2 =
                    new ExponentiallyDecayingEmergingItemsets(inlierItemSummarySize,
                                                              outlierItemSummarySize,
                                                              minSupport,
                                                              minOIRatio,
                                                              decayRate,
                                                              attributes.size(),
                                                              true);

            for(int pt = 0; pt < scored.size(); ++pt) {
                OutlierClassificationResult p = scored.get(pt);
                if(p.isOutlier()){
                    testSummarizer.markOutlier(p.getDatum());
                } else {
                    testSummarizer.markInlier(p.getDatum());
                }

                if(pt % PERIOD == 0) {
                    testSummarizer.markPeriod();
                }
            }

            List<ItemsetResult> isr2 = testSummarizer2.getItemsets(conf.getEncoder());
            sw.stop();
            log.info("FUZZYCPS: {} {}", sw.elapsed(TimeUnit.MICROSECONDS), isr2.size());
            for(ItemsetResult is: isr2) {
                log.info("FUZZYCPS: {} {} {} {}", is.getItems().size(), is.getSupport(), is.getRatioToInliers(), is.getNumRecords());
                for(ColumnValue cv : is.getItems()) {
                    log.info("{} {}", cv.getColumn(), cv.getValue());
                }

                log.info("FUZZY_ITEMS: {}", is.getItems().stream().map(a -> a.getColumn()+":"+a.getValue()).collect(
                        Collectors.joining(",")));
                log.info("");
            }
            sw.reset();

        }

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}
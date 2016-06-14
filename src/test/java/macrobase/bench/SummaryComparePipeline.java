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

public class SummaryComparePipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(SummaryComparePipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw1 = Stopwatch.createStarted();
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
            if(!d.isOutlier()) {
                in_txns.add(Sets.newHashSet(d.getDatum().getAttributes()));
                inliers.add(d);
                inlier_data.add(d.getDatum());
            } else {
                out_txns.add(Sets.newHashSet(d.getDatum().getAttributes()));
                outliers.add(d);
                outlier_data.add(d.getDatum());
            }
        }
        final int iterations = 5;
        long timeout_ms = 20;

        ExecutorService st = Executors.newCachedThreadPool();

        for(int i = 0; i < iterations; ++i) {
            Stopwatch sw = Stopwatch.createStarted();

            FPGrowthEmerging fpg = new FPGrowthEmerging(true);

            fpg.getEmergingItemsetsWithMinSupport(inlier_data,
                                                  outlier_data,
                                                  .001,
                                                  3,
                                                  conf.getEncoder());
            sw.stop();
            long fpge = sw.elapsed(TimeUnit.MICROSECONDS);
            log.debug("fpge took {}", fpge);


            timeout_ms = sw.elapsed(TimeUnit.MILLISECONDS)*1200;
            sw.reset();


            System.gc();
        }

        for(int i = 0; i < iterations; ++i) {
            Stopwatch sw = Stopwatch.createStarted();

            FPGrowth out_fpg = new FPGrowth();
            List<ItemsetWithCount> outlier_items = out_fpg.getItemsetsWithSupportRatio(out_txns, .001);
            FPGrowth in_fpg = new FPGrowth();
            List<ItemsetWithCount> inlier_items = in_fpg.getItemsetsWithSupportRatio(in_txns, .001 / 3);

            log.debug("fpg-vanilla took {}", sw.elapsed(TimeUnit.MICROSECONDS));

            HashMap<Set<Integer>, Double> inlierSetCounts = new HashMap<>();
            HashMap<Integer, Double> inlierItemCounts = new HashMap<>();
            HashMap<Integer, Double> outlierItemCounts = new HashMap<>();

            for(ItemsetWithCount inlierItem : inlier_items) {
                inlierSetCounts.put(inlierItem.getItems(), inlierItem.getCount());
                if(inlierItem.getItems().size() == 1) {
                    inlierItemCounts.put(inlierItem.getItems().iterator().next(), inlierItem.getCount());
                }
            }

            Set<ItemsetWithCount> outliersToCheck = new HashSet<>();

            for(ItemsetWithCount outlierItem : outlier_items) {
                if(outlierItem.getItems().size() == 1) {
                    outlierItemCounts.put(outlierItem.getItems().iterator().next(), outlierItem.getCount());
                } else {
                    outliersToCheck.add(outlierItem);
                }
            }

            Set<ItemsetWithCount> ret = new HashSet<>();
            Set<Integer> supportedItems = new HashSet<>();

            for(Map.Entry<Integer, Double> oi : outlierItemCounts.entrySet()) {
                Double inlierCount = inlierItemCounts.get(oi.getKey());
                if(inlierCount == null ||
                   (oi.getValue()/out_txns.size())/(inlierCount/in_txns.size())> 3) {
                    supportedItems.add(oi.getKey());
                    ret.add(new ItemsetWithCount(Sets.newHashSet(oi.getKey()), oi.getValue()));
                }
            }

            for(ItemsetWithCount c : outliersToCheck) {
                boolean good = true;
                for(Integer item : c.getItems()) {
                    if(!supportedItems.contains(item)) {
                        good = false;
                        break;
                    }
                }

                if(!good) {
                    continue;
                }

                Double inlierCount = inlierSetCounts.get(c.getItems());
                if(inlierCount == null ||
                   (c.getCount()/out_txns.size())/(inlierCount/in_txns.size())> 3) {
                    ret.add(c);
                }
            }


            sw.stop();
            log.debug("fpg-ratio took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

        }

        for(int i = 0; i < iterations; ++i) {
            Stopwatch sw = Stopwatch.createStarted();

            Future r = st.submit((Runnable) () -> {
                CubeCompare cub = new CubeCompare();
                cub.compare(inliers);
                CubeCompare cubo = new CubeCompare();
                cubo.compare(outliers);
            });

            try {
                r.get(timeout_ms, TimeUnit.MILLISECONDS);
                r.cancel(true);
            } catch (Exception e) {
                log.debug("caught {}", e);
            }

            sw.stop();
            log.debug("cube took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            System.gc();

        }


        for(int i = 0; i < iterations; ++i) {


            Stopwatch sw = Stopwatch.createStarted();

            Future r = st.submit((Runnable) () -> {
                DecisionTreeCompare dtc = new DecisionTreeCompare(100);
                dtc.compare(inlier_data, outlier_data);
            });

            try {
                r.get(timeout_ms, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.debug("caught {}", e);
                r.cancel(true);
            }


            sw.stop();
            log.debug("dtc100 took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            System.gc();

        }

        for(int i = 0; i < iterations; ++i) {
            Stopwatch sw = Stopwatch.createStarted();

            Future r = st.submit((Runnable) () -> {
                DecisionTreeCompare dtc = new DecisionTreeCompare(10);
                dtc.compare(inlier_data, outlier_data);
            });

            try {
                r.get(timeout_ms, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.debug("caught {}", e);
            }

            try {
                r.get(timeout_ms, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.debug("caught {}", e);
                r.cancel(true);
            }


            sw.stop();
            log.debug("dtc10 took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            System.gc();

        }




        for(int i = 0; i < iterations; ++i) {
            Stopwatch sw = Stopwatch.createStarted();


            System.gc();

            Future r = st.submit((Runnable) () -> {
                Apriori out_apriori = new Apriori();
                out_apriori.getItemsets(out_txns, .001);
                Apriori in_apriori = new Apriori();
                in_apriori.getItemsets(in_txns, .001);
            });

            try {
                r.get(timeout_ms, TimeUnit.MILLISECONDS);
                r.cancel(true);
            } catch (Exception e) {
                log.debug("caught {}", e);
            }

            sw.stop();
            log.debug("apriori took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            System.gc();

        }


        for(int i = 0; i < iterations; ++i) {

            Stopwatch sw = Stopwatch.createStarted();

            Future r = st.submit((Runnable) () -> {
                DataXRayCompare xr = new DataXRayCompare();
                xr.compare(inlier_data, outlier_data);
            });

            try {
                r.get(timeout_ms, TimeUnit.MILLISECONDS);
                r.cancel(true);
            } catch (Exception e) {
                log.debug("caught {}", e);
            }


            sw.stop();
            log.debug("xr took {}", sw.elapsed(TimeUnit.MICROSECONDS));
            sw.reset();

            System.gc();

        }

        System.exit(0);

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}
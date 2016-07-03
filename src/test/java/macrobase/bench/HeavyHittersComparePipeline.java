package macrobase.bench;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.count.AmortizedMaintenanceCounter;
import macrobase.analysis.summary.count.ApproximateCount;
import macrobase.analysis.summary.count.SpaceSavingList;
import macrobase.analysis.summary.itemset.Apriori;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.bench.compare.itemcount.SpaceSavingHeap;
import macrobase.bench.compare.summary.CubeCompare;
import macrobase.bench.compare.summary.DataXRayCompare;
import macrobase.bench.compare.summary.DecisionTreeCompare;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class HeavyHittersComparePipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(HeavyHittersComparePipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    private long bench_native_decay(List<Datum> data, ApproximateCount count, int refreshRate) {
        Stopwatch tsw = Stopwatch.createUnstarted();
        tsw.start();
        for(int tup = 0; tup < data.size(); ++tup) {
            for(Integer attr : data.get(tup).getAttributes()) {
                count.observe(attr, 1);
            }

            if(tup % refreshRate == 0) {
                count.multiplyAllCounts(.95);
            }
        }
        tsw.stop();
        return tsw.elapsed(TimeUnit.MICROSECONDS);
    }

    private double compute_are(Map<Integer, Double> actual, Map<Integer, Double> appx) {
        double ret = 0;
        for(Map.Entry<Integer, Double> ae : actual.entrySet()) {
            ret += Math.abs(ae.getValue()-appx.get(ae.getKey()));
        }

        return ret / actual.size();
    }

    private long bench_external_decay(List<Datum> data, ApproximateCount count, int refreshRate) {
        Stopwatch tsw = Stopwatch.createUnstarted();
        tsw.start();
        double weight = 1;
        for(int tup = 0; tup < data.size(); ++tup) {
            for(Integer attr : data.get(tup).getAttributes()) {
                count.observe(attr, weight);
            }

            if(tup % refreshRate == 0) {
                weight /= .95;
            }
        }
        tsw.stop();
        return tsw.elapsed(TimeUnit.MICROSECONDS);
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        FeatureTransform normalizer = new LinearMetricNormalizer();
        normalizer.consume(ingester.getStream().drain());
        List<Datum> data = normalizer.getStream().drain();
        System.gc();

        if(data.size() > 1000000)
            data = data.subList(0, 1000000);

        Stopwatch tsw = Stopwatch.createUnstarted();
        tsw.start();


        int[] refreshRates = {100000, 10000, 1000000};
        int[] sizes = {10, 100, 1000, 10000, 100000, 1000000};
        for (int rr : refreshRates) {
            for (int size : sizes) {
                AmortizedMaintenanceCounter trueValue = new AmortizedMaintenanceCounter(1000000000);
                bench_native_decay(data, trueValue, rr);

                List<Double> trueVals = Lists.newArrayList(trueValue.getCounts().values());
                Collections.sort(trueVals);
                Collections.reverse(trueVals);

                double minHHVal = trueVals.get(size);
                Map<Integer, Double> trueHHs = Maps.newHashMap();
                for(Map.Entry<Integer, Double> e : trueHHs.entrySet()) {
                    if(e.getValue() > minHHVal) {
                        trueHHs.put(e.getKey(), e.getValue());
                    }
                }

                for (int i = 0; i < 5; ++i) {
                    SpaceSavingHeap ssh = new SpaceSavingHeap(size);
                    SpaceSavingList ssl = new SpaceSavingList(size);
                    AmortizedMaintenanceCounter fbsl = new AmortizedMaintenanceCounter(size);

                    log.debug("DATASIZE: {}", data.size());
                    log.debug("SSH {} {} {} {}", size, rr, bench_native_decay(data, ssh, rr), compute_are(trueHHs, ssh.getCounts()));
                    log.debug("SSL {} {} {} {}", size, rr, bench_native_decay(data, ssl, rr), compute_are(trueHHs,
                                                                                                          ssl.getCounts()));
                    log.debug("FBSL {} {} {} {}", size, rr, bench_native_decay(data, fbsl, rr), compute_are(trueHHs,
                                                                                                            fbsl.getCounts()));
                }

                trueValue = new AmortizedMaintenanceCounter(1000000000);
                bench_external_decay(data, trueValue, rr);

                trueVals = Lists.newArrayList(trueValue.getCounts().values());
                Collections.sort(trueVals);
                Collections.reverse(trueVals);

                minHHVal = trueVals.get(size);
                trueHHs = Maps.newHashMap();
                for(Map.Entry<Integer, Double> e : trueHHs.entrySet()) {
                    if(e.getValue() > minHHVal) {
                        trueHHs.put(e.getKey(), e.getValue());
                    }
                }

                double weight = 1;
                for(int tup = 0; tup < data.size(); ++tup) {
                    if(tup % rr == 0) {
                        weight /= .95;
                    }
                }

                for (int i = 0; i < 5; ++i) {
                    SpaceSavingHeap ssh = new SpaceSavingHeap(size);
                    SpaceSavingList ssl = new SpaceSavingList(size);
                    AmortizedMaintenanceCounter fbsl = new AmortizedMaintenanceCounter(size);

                    log.debug("DATASIZE: {}", data.size());
                    log.debug("SSH_EXT {} {} {} {}", size, rr, bench_external_decay(data, ssh, rr), compute_are(trueHHs, ssh.getCounts())/weight);
                    log.debug("SSL_EXT {} {} {} {}", size, rr, bench_external_decay(data, ssl, rr), compute_are(trueHHs,
                                                                                                          ssl.getCounts())/weight);
                    log.debug("FBSL_EXT {} {} {} {}", size, rr, bench_external_decay(data, fbsl, rr), compute_are(trueHHs, fbsl.getCounts())/weight);
                }
            }
        }

        return Arrays.asList(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>()));
    }
}
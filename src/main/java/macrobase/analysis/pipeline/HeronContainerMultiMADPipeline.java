package macrobase.analysis.pipeline;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.pipeline.operator.MBGroupBy;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.ContributingDatum;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.analysis.transform.ProjectMetric;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.ingest.result.ColumnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HeronContainerMultiMADPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(HeronContainerMultiMADPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    public static final String HERON_GROUP_BY_ATTRS = "heron.groupby.attributes";
    public static final List<String> DEFAULT_HERON_GROUP_BY_ATTRS = Lists.newArrayList("component", "topology");


    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        FeatureTransform normalizer = new LinearMetricNormalizer();
        normalizer.consume(ingester.getStream().drain());
        List<Datum> data = normalizer.getStream().drain();
        System.gc();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        List<String> attrs = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        List<String> metrics = Lists.newArrayList(conf.getStringList(MacroBaseConf.LOW_METRICS));
        metrics.addAll(conf.getStringList(MacroBaseConf.HIGH_METRICS));

        List<String> groupByAttrs = conf.getStringList(HERON_GROUP_BY_ATTRS, DEFAULT_HERON_GROUP_BY_ATTRS);

        List<Integer> groupByIndices = new ArrayList<>();
        for(String a : groupByAttrs) {
            groupByIndices.add(attrs.indexOf(a));
        }

        Map<Integer, List<ItemsetResult>> commonItemsets = new HashMap<>();

        long totalMs = 0, summarizeMs = 0, executeMs = 0;
        double numOutliers = 0;
        double numInliers = 0;

        for(int metricNo = 0; metricNo < metrics.size(); ++metricNo) {

            MBGroupBy gb = new MBGroupBy(groupByIndices,
                                         () -> new BatchScoreFeatureTransform(conf, MacroBaseConf.TransformType.MAD));

            ProjectMetric pm = new ProjectMetric(metricNo);
            pm.consume(data);
            gb.consume(pm.getStream().drain());

            OutlierClassifier oc = new BatchingPercentileClassifier(conf);
            oc.consume(gb.getStream().drain());

            Summarizer bs = new BatchSummarizer(conf);
            bs.consume(oc.getStream().drain());
            final Summary result = bs.summarize().getStream().drain().get(0);

            totalMs += sw.elapsed(TimeUnit.MILLISECONDS) - loadMs;
            summarizeMs += result.getCreationTimeMs();
            executeMs += totalMs - result.getCreationTimeMs();

            numInliers += result.getNumInliers();
            numOutliers += result.getNumOutliers();

            log.info("dim {} took {}ms ({} tuples/sec)",
                     metricNo,
                     sw.elapsed(TimeUnit.MILLISECONDS) - loadMs,
                     (result.getNumInliers() + result.getNumOutliers()) / (double) totalMs * 1000);

            for (ItemsetResult r : result.getItemsets()) {
                List<ItemsetResult> matchingItemsets = commonItemsets.get(r.getItems().hashCode());

                if (matchingItemsets == null) {
                    matchingItemsets = new ArrayList<>();
                    commonItemsets.put(r.getItems().hashCode(), matchingItemsets);
                }

                matchingItemsets.add(r);
                r.additional = metrics.get(metricNo);
            }
        }

        List<ItemsetResult> combinedItemsets = new ArrayList<>();

        for (List<ItemsetResult> isrs : commonItemsets.values()) {
            double sumOutliers = 0, sumRecords = 0, sumRatios = 0;
            for (ItemsetResult item : isrs) {
                sumOutliers += item.getNumRecords() / item.getSupport();
                sumRecords += item.getNumRecords();
                sumRatios += item.getRatioToInliers();
            }

            ItemsetResult combined = new ItemsetResult(sumRecords / sumOutliers,
                                                       sumRecords / isrs.size(),
                                                       sumRatios / isrs.size(),
                                                       isrs.get(0).getItems());

            combined.additional = "anomalous metrics: "+
                                  Joiner.on(", ").join(isrs.stream()
                                                               .map(a -> a.additional)
                                                               .collect(Collectors.toList()));

            combinedItemsets.add(combined);
        }

        return Arrays.asList(new AnalysisResult(numOutliers / metrics.size(),
                numInliers / metrics.size(),
                loadMs,
                executeMs,
                summarizeMs,
                combinedItemsets));
    }
}
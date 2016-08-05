package macrobase.analysis.pipeline;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.pipeline.operator.MBGroupBy;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.ContributingDatum;
import macrobase.analysis.stats.MinCovDet;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
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

public class HeronContainerPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(HeronContainerPipeline.class);

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

        MBGroupBy gb = new MBGroupBy(groupByIndices,
                                     () -> new BatchScoreFeatureTransform(conf, conf.getTransformType()));

        gb.consume(data);

        OutlierClassifier oc = new BatchingPercentileClassifier(conf);

        oc.consume(gb.getStream().drain());

        Summarizer bs = new BatchSummarizer(conf);
        List<OutlierClassificationResult> ocr = oc.getStream().drain();

        Map<Integer, double[]> contributionMap = new HashMap<>();

        for(OutlierClassificationResult r : ocr) {
            if(r.isOutlier()) {
                ContributingDatum cd = (ContributingDatum)r.getDatum();
                for(Integer attrNo : cd.attributes()) {
                    double[] contributionCount = contributionMap.get(attrNo);
                    if(contributionCount == null) {
                        contributionCount = new double[cd.contribution.length];
                        contributionMap.put(attrNo, contributionCount);
                    }

                    for(int i = 0; i < cd.contribution.length; ++i) {
                        contributionCount[i] += cd.contribution[i];
                    }
                }
            }
        }

        bs.consume(ocr);
        Summary result = bs.summarize().getStream().drain().get(0);

        final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS) - loadMs;
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - result.getCreationTimeMs();

        log.info("took {}ms ({} tuples/sec)",
                totalMs,
                (result.getNumInliers() + result.getNumOutliers()) / (double) totalMs * 1000);

        for(ItemsetResult r : result.getItemsets()) {
            // store them in commonCounts
            int[] commonCounts = new int[metrics.size()];
            for(ColumnValue cv : r.getItems()) {
                Integer encodedColumn = conf.getEncoder().getIntegerEncoding(attrs, cv);
                List<Double> contributions = Doubles.asList(contributionMap.get(encodedColumn));
                // find the indices that contribute 90% of the MCD entries
                List<Integer> sortedIndices = IntStream.range(0, contributions.size())
                        .boxed().sorted((i, j) -> -contributions.get(i).compareTo(contributions.get(j)))
                        .mapToInt(ele -> ele).boxed().collect(Collectors.toList());

                Double sumContributions = contributions.stream().reduce((a, b) -> a + b).get();

                double runningSum = 0;
                for(Integer sortedIndex : sortedIndices) {
                    commonCounts[sortedIndex] += 1;

                    runningSum += contributions.get(sortedIndex);
                    if(runningSum > sumContributions*.9) {
                        break;
                    }
                }
            }

            // if a contributing index appears in more than 50% of the columnvalues
            // add it to the summary
            List<String> contributingMetrics = new ArrayList<>();
            for(int i = 0; i < commonCounts.length; ++i) {
                if(commonCounts[i] > r.getItems().size()/2.) {
                    contributingMetrics.add(metrics.get(i));
                }
            }

            if(!contributingMetrics.isEmpty()) {
                r.additional = "anomalous metrics: "+
                                            Joiner.on(", ").join(contributingMetrics);
            }
        }

        return Arrays.asList(new AnalysisResult(result.getNumOutliers(),
                result.getNumInliers(),
                loadMs,
                executeMs,
                summarizeMs,
                result.getItemsets()));
    }
}
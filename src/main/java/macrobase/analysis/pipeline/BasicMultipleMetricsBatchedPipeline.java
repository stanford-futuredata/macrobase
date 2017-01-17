package macrobase.analysis.pipeline;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.classify.StaticThresholdClassifier;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.MAD;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseConf.TransformType;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.io.*;

public class BasicMultipleMetricsBatchedPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicMultipleMetricsBatchedPipeline.class);

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
        List<Datum> data = ingester.getStream().drain();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        int numberOfMetrics = data.get(0).getMetrics().getDimension();
        
        Map<ItemsetResult, String> isr2GeneratingMetric = new HashMap<ItemsetResult, String>();
        
        for (int m = 0; m < numberOfMetrics; m++) {
            
            List<Datum> dataWithOneMetric = new ArrayList<Datum>();
            for (Datum d: data) {
                Datum newD = new Datum(d, d.getMetrics().getEntry(m));
                dataWithOneMetric.add(newD);
            }
            
            BatchScoreFeatureTransform ft = new BatchScoreFeatureTransform(conf, conf.getTransformType());
            ft.consume(dataWithOneMetric);
            
            OutlierClassifier outlierClassifier = new StaticThresholdClassifier(conf);
            outlierClassifier.consume(ft.getStream().drain());
            
            List<OutlierClassificationResult> outlierClassificationResults = outlierClassifier.getStream().drain();
            
            List<Integer> outlierIndexes = new ArrayList<Integer>();
            List<Integer> outlierMADHighIndexes = new ArrayList<Integer>();
            List<Integer> outlierMADLowIndexes = new ArrayList<Integer>();
            
            for (int i = 0; i < outlierClassificationResults.size(); i++) {
                OutlierClassificationResult outlierClassificationResult = outlierClassificationResults.get(i);
                if (outlierClassificationResult.isOutlier()) {
                    outlierIndexes.add(i);    
                    
                    if (conf.getTransformType() == TransformType.MAD) {
                        MAD mad = (MAD)ft.getBatchTrainScore();
                        if (dataWithOneMetric.get(i).getMetrics().getEntry(0) > mad.getMedian()) {
                            outlierMADHighIndexes.add(i);
                        } else if (dataWithOneMetric.get(i).getMetrics().getEntry(0) < mad.getMedian()) {
                            outlierMADLowIndexes.add(i);
                        }
                    }
                    
                }
            }
            
            if (outlierMADLowIndexes.size() > 0) {
                continue;
            }
            
            String generatingMetric = "LogID " + m + " has number of outliers: " + outlierIndexes.size() + 
                    //" high metric outliers: " + outlierMADHighIndexes.size() + 
                    //" low metric outliers: " + outlierMADLowIndexes.size() + 
                    " and number of inliners: " + (data.size() - outlierIndexes.size());
            
            Summarizer bs = new BatchSummarizer(conf);
            bs.consume(outlierClassificationResults);
            Summary result = bs.getStream().drain().get(0);
            
            for (ItemsetResult isr: result.getItemsets()) {
                isr2GeneratingMetric.put(isr, generatingMetric);
            }
        }
        
        
        List<ItemsetResult> allisrs = new ArrayList<ItemsetResult>(isr2GeneratingMetric.keySet());
        allisrs.sort(new Comparator<ItemsetResult>(){
            @Override
            public int compare(ItemsetResult o1, ItemsetResult o2) {
                if (o1.getSupport() > o2.getSupport()){
                    return -1;
                } else if (o1.getSupport() < o2.getSupport()) {
                    return 1;
                } else {
                   if (o1.getNumRecords() > o2.getNumRecords()) {
                       return -1;
                   } else if (o1.getNumRecords() < o2.getNumRecords()) {
                       return 1;
                   } else {
                       if (o1.getRatioToInliers() > o2.getRatioToInliers()){
                           return -1;
                       } else if (o1.getRatioToInliers() < o2.getRatioToInliers()) {
                           return 1;
                       } else {
                           return 0;
                       }
                   }
                }
            }});
        
        PrintWriter pw1 = new PrintWriter("/Users/xuchu/Dropbox/PHD Projects/OutlierDetection/Datasets/SamLogData/Oct20/rankedBySupport.txt");
        for (int i = 0; i < allisrs.size(); i++) {
            pw1.println("************************");
            pw1.println( isr2GeneratingMetric.get(allisrs.get(i)) + "\n" + allisrs.get(i).prettyPrint() );
        }
        pw1.close();
        
        
        allisrs = new ArrayList<ItemsetResult>(isr2GeneratingMetric.keySet());
        allisrs.sort(new Comparator<ItemsetResult>(){
            @Override
            public int compare(ItemsetResult o1, ItemsetResult o2) {
                if (o1.getRatioToInliers() > o2.getRatioToInliers()){
                    return -1;
                } else if (o1.getRatioToInliers() < o2.getRatioToInliers()) {
                    return 1;
                } else {
                    if (o1.getSupport() > o2.getSupport()){
                        return -1;
                    } else if (o1.getSupport() < o2.getSupport()) {
                        return 1;
                    } else {
                        if (o1.getNumRecords() > o2.getNumRecords()) {
                            return -1;
                        } else if (o1.getNumRecords() < o2.getNumRecords()) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                }
            }});
        
        PrintWriter pw2 = new PrintWriter("/Users/xuchu/Dropbox/PHD Projects/OutlierDetection/Datasets/SamLogData/Oct20/rankedByOutlierInlierRatio.txt");
        for (int i = 0; i < allisrs.size(); i++) {
            pw2.println("************************");
            pw2.println( isr2GeneratingMetric.get(allisrs.get(i)) + "\n" + allisrs.get(i).prettyPrint() );
        }
        pw2.close();
        
        
        allisrs = new ArrayList<ItemsetResult>(isr2GeneratingMetric.keySet());
        allisrs.sort(new Comparator<ItemsetResult>(){
            @Override
            public int compare(ItemsetResult o1, ItemsetResult o2) {
                if (o1.getNumRecords() > o2.getNumRecords()){
                    return -1;
                } else if (o1.getNumRecords() < o2.getNumRecords()) {
                    return 1;
                } else {
                    if (o1.getSupport() > o2.getSupport()){
                        return -1;
                    } else if (o1.getSupport() < o2.getSupport()) {
                        return 1;
                    } else {
                        if (o1.getRatioToInliers() > o2.getRatioToInliers()) {
                            return -1;
                        } else if (o1.getRatioToInliers() < o2.getRatioToInliers()) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                }
            }});
        
        PrintWriter pw3 = new PrintWriter("/Users/xuchu/Dropbox/PHD Projects/OutlierDetection/Datasets/SamLogData/Oct20/rankedByNumberOfOutliers.txt");
        for (int i = 0; i < allisrs.size(); i++) {
            pw3.println("************************");
            pw3.println( isr2GeneratingMetric.get(allisrs.get(i)) + "\n" + allisrs.get(i).prettyPrint() );
        }
        pw3.close();
        
        /*
        OutlierClassifier oc = new BatchingPercentileClassifier(conf);

        if (conf.getBoolean(MacroBaseConf.CLASSIFIER_DUMP)) {
            String queryName = conf.getString(MacroBaseConf.QUERY_NAME);
            oc = new DumpClassifier(conf, oc, queryName);
        }

        oc.consume(ft.getStream().drain());

        Summarizer bs = new BatchSummarizer(conf);
        bs.consume(oc.getStream().drain());

        Summary result = bs.getStream().drain().get(0);

        final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS) - loadMs;
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
                */
        return new ArrayList<AnalysisResult>();
    }
    
    
    
}
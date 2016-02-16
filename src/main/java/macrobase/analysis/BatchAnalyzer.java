package macrobase.analysis;

import com.google.common.base.Stopwatch;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;

import macrobase.ingest.transform.DataTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BatchAnalyzer extends BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(BatchAnalyzer.class);

    public BatchAnalyzer(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
        conf.sanityCheckBatch();
    }

    public AnalysisResult analyze() throws SQLException, IOException, ConfigurationException {
        DatumEncoder encoder = new DatumEncoder();

        Stopwatch sw = Stopwatch.createUnstarted();

        // OUTLIER ANALYSIS
        log.debug("Starting loading...");
        sw.start();
        List<Datum> data = constructLoader().getData(encoder);
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended loading (time: {}ms)!", loadTime);
        
        Stopwatch tsw = Stopwatch.createUnstarted();
        Stopwatch tsw2 = Stopwatch.createUnstarted();
        tsw.start();
        tsw2.start();

        sw.start();

        OutlierDetector detector = constructDetector(randomSeed);

        OutlierDetector.BatchResult or;
        if(forceUsePercentile || (!forceUseZScore && targetPercentile > 0)) {
            or = detector.classifyBatchByPercentile(data, targetPercentile);
        } else {
            or = detector.classifyBatchByZScoreEquivalent(data, zScore);
        }
        sw.stop();

        long classifyTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        tsw2.stop();

        // SUMMARY

        final int inlierSize = or.getInliers().size();
        final int outlierSize = or.getOutliers().size();

        log.debug("Starting summarization...");

        sw.start();
        FPGrowthEmerging fpg = new FPGrowthEmerging();
        List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(or.getInliers(),
                                                                        or.getOutliers(),
                                                                        minSupport,
                                                                        minOIRatio,
                                                                        encoder);
        sw.stop();
        tsw.stop();
        
        double tuplesPerSecond = ((double) data.size()) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;
        
        long summarizeTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("...ended summarization (time: {}ms)!", summarizeTime);

        log.debug("Number of itemsets: {}", isr.size());
        log.debug("...ended total (time: {}ms)!", (tsw.elapsed(TimeUnit.MICROSECONDS) / 1000) + 1);
        log.debug("Tuples / second = {} tuples / second", tuplesPerSecond);

        tuplesPerSecond = ((double) data.size()) / ((double) tsw2.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;
        log.debug("Tuples / second w/o itemset mining = {} tuples / second", tuplesPerSecond);

        return new AnalysisResult(outlierSize, inlierSize, loadTime, classifyTime, summarizeTime, isr);
    }
}

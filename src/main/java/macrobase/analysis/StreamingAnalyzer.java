package macrobase.analysis;

import com.google.common.base.Stopwatch;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.periodic.AbstractPeriodicUpdater;
import macrobase.analysis.periodic.TupleBasedRetrainer;
import macrobase.analysis.periodic.TupleAnalysisDecayer;
import macrobase.analysis.periodic.WallClockRetrainer;
import macrobase.analysis.periodic.WallClockAnalysisDecayer;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.SQLLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class StreamingAnalyzer extends BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(StreamingAnalyzer.class);

    private Integer warmupCount;
    private Integer inputReservoirSize;
    private Integer scoreReservoirSize;
    private Integer summaryPeriod;
    private Boolean useRealTimePeriod;
    @SuppressWarnings("unused")
	private Boolean useTupleCountPeriod;
    private double decayRate;
    private Integer modelRefreshPeriod;

    private double minSupportOutlier;
    private double minRatio;
    private Integer outlierItemSummarySize;
    private Integer inlierItemSummarySize;
    
    private Semaphore startSemaphore;
    private Semaphore endSemaphore;

    public void setModelRefreshPeriod(Integer modelRefreshPeriod) {
        this.modelRefreshPeriod = modelRefreshPeriod;
    }

    public void setInputReservoirSize(Integer inputReservoirSize) {
        this.inputReservoirSize = inputReservoirSize;
    }

    public void setScoreReservoirSize(Integer scoreReservoirSize) {
        this.scoreReservoirSize = scoreReservoirSize;
    }

    public void setSummaryPeriod(Integer summaryPeriod) {
        this.summaryPeriod = summaryPeriod;
    }

    public void useRealTimeDecay(Boolean useRealTimeDecay) {
        this.useRealTimePeriod = useRealTimeDecay;
    }

    public void useTupleCountDecay(Boolean useTupleCountDecay) {
        this.useTupleCountPeriod = useTupleCountDecay;
    }

    public void setDecayRate(double decayRate) {
        this.decayRate = decayRate;
    }

    public void setMinSupportOutlier(double minSupportOutlier) {
        this.minSupportOutlier = minSupportOutlier;
    }

    public void setMinRatio(double minRatio) {
        this.minRatio = minRatio;
    }

    public void setTracing(boolean doTrace) {
        this.doTrace = doTrace;
    }

    public void setOutlierItemSummarySize(Integer outlierItemSummarySize) {
        this.outlierItemSummarySize = outlierItemSummarySize;
    }

    public void setInlierItemSummarySize(Integer inlierItemSummarySize) {
        this.inlierItemSummarySize = inlierItemSummarySize;
    }

    boolean doTrace;
    int numRuns = 100000;

    class RunnableStreamingAnalysis implements Runnable {
    	List<Datum> data;
    	List<String> attributes;
    	List<String> lowMetrics;
    	List<String> highMetrics;
    	String baseQuery;
    	DatumEncoder encoder;
    	List<ItemsetResult> itemsetResults;
        int numThreads;

    	RunnableStreamingAnalysis(int numThreads) {
                this.numThreads = numThreads;
    	}
    	
    	public List<ItemsetResult> getItemsetResults() {
    		return itemsetResults;
    	}
    	
        @Override
        public void run() {
            int a = 1;
            Stopwatch sw = Stopwatch.createUnstarted();
            Stopwatch tsw = Stopwatch.createUnstarted();
            tsw.start();
            try {
                startSemaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sw.start();
            long numIterations = (1000000000 / numThreads);
            for (long i = 0; i < numIterations; i++) {
              a *= i;
              a -= i;
            }
            sw.stop();
            log.debug("Only-computation time: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
            endSemaphore.release();
            tsw.stop();
            log.debug("Total time: {}ms", tsw.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    public AnalysisResult analyzeOnePass(SQLLoader loader,
                                              List<String> attributes,
                                              List<String> lowMetrics,
                                              List<String> highMetrics,
                                              String baseQuery) throws SQLException, IOException, InterruptedException {
    	DatumEncoder encoder = new DatumEncoder();

        Stopwatch tsw = Stopwatch.createUnstarted();
    	tsw.start();

        startSemaphore = new Semaphore(0);
        endSemaphore = new Semaphore(0);
        
        List<ItemsetResult> isr = new ArrayList<ItemsetResult>();
        for (int i = 0; i < numThreads; i++) {
        	RunnableStreamingAnalysis rsa = new RunnableStreamingAnalysis(
        			numThreads);
        	Thread t = new Thread(rsa);
        	t.start();
        }
        
        startSemaphore.release(numThreads);
        endSemaphore.acquire(numThreads);

        tsw.stop();
        
        double tuplesPerSecond = (1000000000 / ((double) tsw.elapsed(TimeUnit.MICROSECONDS)));
        tuplesPerSecond *= 1000000;
        
        log.debug("Net tuples / second = {} tuples / second", tuplesPerSecond);

        return new AnalysisResult(0, 0, 0, 0, 0, isr);
    }

    public void setWarmupCount(Integer warmupCount) {
        this.warmupCount = warmupCount;
    }
}

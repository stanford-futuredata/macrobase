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
import macrobase.runtime.standalone.BaseStandaloneConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
    int numRuns = 40;
    
    CopyOnWriteArrayList<Double> perThreadMedians;
    
    public StreamingAnalyzer(BaseStandaloneConfiguration configuration) 
    {
    	super(configuration);
        perThreadMedians = new CopyOnWriteArrayList<Double> ();
    }

    class RunnableStreamingAnalysis implements Runnable {
    	List<Datum> data;
    	List<String> attributes;
    	List<String> lowMetrics;
    	List<String> highMetrics;
    	String baseQuery;
    	DatumEncoder encoder;
    	List<ItemsetResult> itemsetResults;
    	
    	int threadId;

    	RunnableStreamingAnalysis(List<Datum> data, List<String> attributes,
    			List<String> lowMetrics, List<String> highMetrics, String baseQuery,
    			DatumEncoder encoder, int threadId) {
    		this.data = data;
    		this.attributes = attributes;
    		this.lowMetrics = lowMetrics;
    		this.highMetrics = highMetrics;
    		this.baseQuery = baseQuery;
    		this.encoder = encoder;
    		
    		this.threadId = threadId;
    	}
    	
    	public List<ItemsetResult> getItemsetResults() {
    		return itemsetResults;
    	}
    	
        @Override
        public void run() {
        	int tupleNo = 0;
        	long totODTrainingTime = 0;
        	long totSummarizationTrainingTime = 0;
        	long totScoringTime = 0;
        	long totSummarizationTime = 0;

	        Stopwatch sw = Stopwatch.createUnstarted();
	        Stopwatch tsw = Stopwatch.createUnstarted();

	        // OUTLIER ANALYSIS
	        tsw.start();

	        int metricsDimensions = lowMetrics.size() + highMetrics.size();
	        OutlierDetector detector = constructDetector(metricsDimensions);

	        ExponentiallyBiasedAChao<Datum> inputReservoir =
	                new ExponentiallyBiasedAChao<>(inputReservoirSize, decayRate);

	        ExponentiallyBiasedAChao<Double> scoreReservoir = null;

	        if(forceUsePercentile) {
	            scoreReservoir = new ExponentiallyBiasedAChao<>(scoreReservoirSize, decayRate);
	        }

	        ExponentiallyDecayingEmergingItemsets streamingSummarizer =
	                new ExponentiallyDecayingEmergingItemsets(inlierItemSummarySize,
	                                                          outlierItemSummarySize,
	                                                          minSupportOutlier,
	                                                          minRatio,
	                                                          decayRate);

	        AbstractPeriodicUpdater analysisUpdater;
	        if(useRealTimePeriod) {
	            analysisUpdater = new WallClockAnalysisDecayer(System.currentTimeMillis(),
	                                                                 summaryPeriod,
	                                                                 inputReservoir,
	                                                                 scoreReservoir,
	                                                                 detector,
	                                                                 streamingSummarizer);
	        } else {
	            analysisUpdater = new TupleAnalysisDecayer(summaryPeriod,
	                                                             inputReservoir,
	                                                             scoreReservoir,
	                                                             detector,
	                                                             streamingSummarizer);
	        }

	        AbstractPeriodicUpdater modelUpdater;
	        if(useRealTimePeriod) {
	            modelUpdater = new WallClockRetrainer(System.currentTimeMillis(),
	                                                        modelRefreshPeriod,
	                                                        inputReservoir,
	                                                        detector,
	                                                        streamingSummarizer);
	        } else {
	            modelUpdater = new TupleBasedRetrainer(modelRefreshPeriod,
	                                                         inputReservoir,
	                                                         detector,
	                                                         streamingSummarizer);
	        }

	        tupleNo = 0;
	        totODTrainingTime = 0;
	        totSummarizationTrainingTime = 0;
	        totScoringTime = 0;
	        totSummarizationTime = 0;
	        
	        for (int i = 0; i < numRuns; i++) {
		        for(Datum d: data) {
		            inputReservoir.insert(d);
	
		            if(tupleNo == warmupCount) {
		            	sw.start();
		                detector.train(inputReservoir.getReservoir(), null);
		                for(Datum id : inputReservoir.getReservoir()) {
		                    scoreReservoir.insert(detector.score(id));
		                }
		                detector.updateRecentScoreList(scoreReservoir.getReservoir());
		                sw.stop();
		                sw.reset();
		                log.debug("...ended warmup training (time: {}ms)!", sw.elapsed(TimeUnit.MILLISECONDS));
		            } else if(tupleNo >= warmupCount) {
	                    long now = useRealTimePeriod ? System.currentTimeMillis() : 0;

	                    analysisUpdater.updateIfNecessary(now, tupleNo);
	                    modelUpdater.updateIfNecessary(now, tupleNo);
	                    double score = detector.score(d);

	                    if(scoreReservoir != null) {
	                        scoreReservoir.insert(score);
	                    }

	                    if((forceUseZScore && detector.isZScoreOutlier(score, ZSCORE)) ||
	                       forceUsePercentile && detector.isPercentileOutlier(score,
	                                                                          TARGET_PERCENTILE)) {
	                        streamingSummarizer.markOutlier(d);
	                    } else {
	                        streamingSummarizer.markInlier(d);
	                    }
		            }
	
		            tupleNo += 1;
		        }
		    }

	        sw.start();
	        List<ItemsetResult> isr = streamingSummarizer.getItemsets(encoder);
	        sw.stop();
	        totSummarizationTime += sw.elapsed(TimeUnit.MICROSECONDS);
	        sw.reset();
	        tsw.stop();
	        
	        double tuplesPerSecond = ((double) data.size() * numRuns) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
	        tuplesPerSecond *= 1000000;
			
	        log.debug("...ended OD training (time: {}ms)!", (totODTrainingTime / 1000) + 1);
	        log.debug("...ended summarization training (time: {}ms)!", (totSummarizationTrainingTime / 1000) + 1);
	        log.debug("...ended scoring (time: {}ms)!", (totScoringTime / 1000) + 1);
	        log.debug("...ended summarization (time: {}ms)!", (totSummarizationTime / 1000) + 1);
	        log.debug("...ended total (time: {}ms)!", (tsw.elapsed(TimeUnit.MICROSECONDS) / 1000) + 1);
	        log.debug("Tuples / second = {} tuples / second", tuplesPerSecond);

	        log.debug("Number of itemsets: {}", isr.size());
	        
	        this.itemsetResults = isr;
		}
    	
    }

    public AnalysisResult analyzeOnePass(SQLLoader loader,
                                              List<String> attributes,
                                              List<String> lowMetrics,
                                              List<String> highMetrics,
                                              String baseQuery) throws SQLException, IOException, InterruptedException {
    	DatumEncoder encoder = new DatumEncoder();

        Stopwatch sw = Stopwatch.createUnstarted();
    	log.debug("Starting loading...");
        sw.start();
        List<Datum> data;
		data = loader.getData(encoder,
			                  attributes,
			                  lowMetrics,
			                  highMetrics,
			                  baseQuery);
	    Collections.shuffle(data);
	    
	    List<ArrayList<Datum>> dataPartitioned = new ArrayList<ArrayList<Datum>>(numThreads);
	    for (int i = 0; i < numThreads; i++) {
	    	dataPartitioned.add(new ArrayList<Datum> ());
	    }
	    for (int i = 0; i < data.size(); i++) {
	    	dataPartitioned.get(i % numThreads).add(data.get(i));
	    }
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended loading (time: {}ms)!", loadTime);
        
        List<Thread> threads = new ArrayList<Thread>();
        List<RunnableStreamingAnalysis> rsas = new ArrayList<RunnableStreamingAnalysis>();
        
        Stopwatch tsw = Stopwatch.createUnstarted();
        tsw.start();
        
        List<ItemsetResult> isr = new ArrayList<ItemsetResult>();
        for (int i = 0; i < numThreads; i++) {
        	RunnableStreamingAnalysis rsa = new RunnableStreamingAnalysis(
        			dataPartitioned.get(i), attributes, lowMetrics, highMetrics,
        			baseQuery, encoder, i);
        	Thread t = new Thread(rsa);
        	t.start();
        	threads.add(t);
        	rsas.add(rsa);
        }

        for (int i = 0; i < numThreads; i++) {
        	threads.get(i).join();
        	for (ItemsetResult itemsetResult : rsas.get(i).getItemsetResults()) {
        		isr.add(itemsetResult);
        	}
        }
        
        tsw.stop();
        
        double tuplesPerSecond = ((double) data.size() * numRuns) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;
        
        log.debug("Net tuples / second = {} tuples / second", tuplesPerSecond);

        return new AnalysisResult(0, 0, 0, 0, 0, isr);
    }

    public void setWarmupCount(Integer warmupCount) {
        this.warmupCount = warmupCount;
    }
}

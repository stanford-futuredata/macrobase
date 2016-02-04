package macrobase.analysis;

import com.google.common.base.Stopwatch;

import macrobase.analysis.outlier.MAD;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.SQLLoader;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.Schema.SchemaColumn;
import macrobase.runtime.standalone.scoping.SubSpaceOutlier;
import macrobase.runtime.standalone.scoping.SubSpaceOutlierDetection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ScopingAnalyzer extends BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(ScopingAnalyzer.class);

    public AnalysisResult analyze(SQLLoader loader,
                                  List<String> attributes,
                                  List<String> lowMetrics,
                                  List<String> highMetrics,
                                  String baseQuery,
                                  List<String> categoricalAttributes,
                                  List<String> numericalAttributes, 
                                  int numInterval,
                                  double minFrequentSubSpaceRatio,
                                  double maxSparseSubSpaceRatio
    			) throws SQLException, IOException {

    	
    	//Need to determine categorical attributes and numerical attributes
    	
    	if(categoricalAttributes.size() == 0 && numericalAttributes.size() == 0){
    		categoricalAttributes = new ArrayList<String>();
        	numericalAttributes = new ArrayList<String>();
        	List<SchemaColumn> schemaColumns = loader.getSchema(baseQuery).getColumns();
    		for(SchemaColumn sc: schemaColumns){
    			String scAttri = sc.getName();
    			String type = sc.getType();
    			System.out.println(scAttri + "\t" + type); 
    			if(type.equals("varchar"))
    				categoricalAttributes.add(scAttri);
    			if(type.equals("float8") || type.equals("numeric") || type.equals("int4"))
    				numericalAttributes.add(scAttri);
    			
    		}
    	}
    	
		log.debug("Categorical attributes are: " + categoricalAttributes.toString());
		log.debug("Numerical attributes are: " + numericalAttributes.toString());
		exploreSubSpaceOutlierDetection(loader, baseQuery, 
				categoricalAttributes, 
				numericalAttributes ,
				numInterval,
				 minFrequentSubSpaceRatio,
                 maxSparseSubSpaceRatio);

    	//use all attributes to scope
    	/*
    	List<String> scopingAttributesFinal = scopingAttributes;
    	if(scopingAttributes.size() == 0){
    		scopingAttributesFinal = new ArrayList<String>();
    		List<SchemaColumn> schemaColumns = loader.getSchema(baseQuery).getColumns();
    		for(SchemaColumn sc: schemaColumns){
    			String scAttri = sc.getName();
    			if(!lowMetrics.contains(scAttri) && !highMetrics.contains(scAttri)){
    				scopingAttributesFinal.add(scAttri);
    			}
    		}
    	}
    	
    	exploreScoping(loader,scopingAttributesFinal, minScopingSupport, attributes, lowMetrics, highMetrics , baseQuery);
		*/
    	
    	DatumEncoder encoder = new DatumEncoder();

        Stopwatch sw = Stopwatch.createUnstarted();

        // OUTLIER ANALYSIS

        log.debug("Starting loading...");
        sw.start();
        List<Datum> data = loader.getData(encoder,
                                          attributes,
                                          lowMetrics,
                                          highMetrics,
                                          baseQuery);
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended loading (time: {}ms)!", loadTime);

        log.debug("Starting classification...");
        sw.start();
        OutlierDetector detector;
        int metricsDimensions = lowMetrics.size() + highMetrics.size();
        if(metricsDimensions == 1) {
            detector = new MAD();
        } else {
            detector = new MinCovDet(metricsDimensions);
        }

        OutlierDetector.BatchResult or;
        if(forceUsePercentile || (!forceUseZScore && TARGET_PERCENTILE > 0)) {
            or = detector.classifyBatchByPercentile(data, TARGET_PERCENTILE);
        } else {
            or = detector.classifyBatchByZScoreEquivalent(data, ZSCORE);
        }
        sw.stop();

        long classifyTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended classification (time: {}ms)!", classifyTime);

        // SUMMARY

        @SuppressWarnings("unused")
		final int supportCountRequired = (int) MIN_SUPPORT*or.getOutliers().size();

        final int inlierSize = or.getInliers().size();
        final int outlierSize = or.getOutliers().size();

        log.debug("Number of outliers " + outlierSize);
        
        log.debug("Starting summarization...");

        sw.start();
        FPGrowthEmerging fpg = new FPGrowthEmerging();
        List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(or.getInliers(),
                                                                        or.getOutliers(),
                                                                        MIN_SUPPORT,
                                                                        MIN_INLIER_RATIO,
                                                                        encoder);
        sw.stop();
        long summarizeTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("...ended summarization (time: {}ms)!", summarizeTime);

        return new AnalysisResult(outlierSize, inlierSize, loadTime, classifyTime, summarizeTime, isr);
    }
    
    private void exploreSubSpaceOutlierDetection(SQLLoader loader,
    		String baseQuery,
    		List<String> categoricalAttributes,
    		List<String> numericalAttributes,
    		 int numInterval,
    		double minFrequentSubSpaceRatio,
            double maxSparseSubSpaceRatio
    		) throws SQLException, IOException{
    	
    	
    	DatumEncoder encoder = new DatumEncoder();
    	log.debug("Starting loading...");
    	Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        List<Datum> data = loader.getData(encoder,
        								  categoricalAttributes,
        								  new ArrayList<String>(),
        								  numericalAttributes,
                                          baseQuery);
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("...ended loading (time: {}ms)!", loadTime);
        
        log.debug("Starting subSpace outlier detection...");
        SubSpaceOutlierDetection subSpaceOutlierDetection = 
        		new SubSpaceOutlierDetection(numInterval,minFrequentSubSpaceRatio,maxSparseSubSpaceRatio,encoder,categoricalAttributes,numericalAttributes);
        List<SubSpaceOutlier> subSpaceOutliers = subSpaceOutlierDetection.run(data);
    }
    
    
    /**
     * Use frequent itemset mining to mine for frequent scopes based on categorical attributes
     * For each frequent scope, run the outlier detection MAD/MCD
     * @param loader
     * @param scopingAttributes
     * @param minScopingSupport
     * @param attributes
     * @param lowMetrics
     * @param highMetrics
     * @param baseQuery
     * @throws SQLException
     */
    /*
    private void exploreScoping(SQLLoader loader,
			  List<String> scopingAttributes,
			  double minScopingSupport,
              List<String> attributes,
              List<String> lowMetrics,
              List<String> highMetrics,
              String baseQuery) throws SQLException{
    	
    	DatumEncoder encoder = new DatumEncoder();
    	log.debug("Starting exploring scoping...");
    	log.debug("Starting loading...");
    	Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        List<Datum> data = loader.getData(encoder,
        								  scopingAttributes,
                                          lowMetrics,
                                          highMetrics,
                                          baseQuery);
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("...ended loading (time: {}ms)!", loadTime);
        
        
    	int metricsDimensions = data.get(0).getMetrics().getDimension();
    	
    	OutlierDetector detector;
        if(metricsDimensions == 1) {
            detector = new MAD();
        } else {
            detector = new MinCovDet(metricsDimensions);
        }
        
        //option 1: iterate all possible scopings
        
        //option 2: use frequent itemset mining to only consider scopes whose support is above a thre
        List<Set<Integer>> txns = new ArrayList<>();
        for(int t = 0; t < data.size(); t++){
        	Set<Integer> txn = new HashSet<Integer>();
        	txn.addAll( data.get(t).getAttributes() );
        	txns.add( txn );
        }
        FPGrowth fp = new FPGrowth();
        List<ItemsetWithCount> itemsets = fp.getItemsets(txns, minScopingSupport);
        for(ItemsetWithCount itemsetWithCount: itemsets){
        	
        	List<ColumnValue> columnValues = encoder.getColsFromAttrSet(itemsetWithCount.getItems());
            List<Datum> scopedData = new ArrayList<Datum>();
        	for(Datum datum: data){
        		if(datum.getAttributes().containsAll(itemsetWithCount.getItems())){
        			scopedData.add(datum);
        		}
        	}
        	OutlierDetector.BatchResult or;
            if(forceUsePercentile || (!forceUseZScore && TARGET_PERCENTILE > 0)) {
                or = detector.classifyBatchByPercentile(scopedData, TARGET_PERCENTILE);
            } else {
                or = detector.classifyBatchByZScoreEquivalent(scopedData, ZSCORE);
            }
            if(or.getOutliers().size() != 0){
            	System.err.println("Scoping: " + 
                        columnValues.stream().map(x -> x.getColumn() + "=" + x.getValue()).collect(Collectors.joining(","))
                       + " with count: " + itemsetWithCount.getCount()		
                       		);
                System.err.println("Outliers Count: " + or.getOutliers().size());
            }
            
        }
        
    }
    */
}

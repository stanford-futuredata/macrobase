package macrobase.analysis;

import com.google.common.base.Stopwatch;

import macrobase.MacroBase;
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
import macrobase.runtime.standalone.scoping.SubSpaceOutlier.ScopeOutlier;
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

    public void analyze(SQLLoader loader,
                                  String baseQuery,
                                  List<String> categoricalAttributes,
                                  List<String> numericalAttributes, 
                                  int numInterval,
                                  double minFrequentSubSpaceRatio,
                                  double maxSparseSubSpaceRatio,
                                  int maxScopeDimensions
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
                 maxSparseSubSpaceRatio,
                 maxScopeDimensions);

    	
    }
    
    private void exploreSubSpaceOutlierDetection(SQLLoader loader,
    		String baseQuery,
    		List<String> categoricalAttributes,
    		List<String> numericalAttributes,
    		 int numInterval,
    		double minFrequentSubSpaceRatio,
            double maxSparseSubSpaceRatio,
            int maxScopeDimensions
    		) throws SQLException, IOException{
    	
    	
    	DatumEncoder encoder = new DatumEncoder();
    	log.debug("Starting loading...");
    	Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        List<Datum> data = loader.getData(encoder,
        								  categoricalAttributes,
        								  new ArrayList<String>(),
        								  numericalAttributes,
        								  new ArrayList<String>(),
        								  null,
                                          baseQuery);
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("...ended loading (time: {}ms)!", loadTime);
        
        
        log.debug("Starting subSpace outlier detection...");
        sw.start();
        
        SubSpaceOutlierDetection subSpaceOutlierDetection = 
        		new SubSpaceOutlierDetection(numInterval,minFrequentSubSpaceRatio,maxSparseSubSpaceRatio,encoder,categoricalAttributes,numericalAttributes,maxScopeDimensions);
        subSpaceOutlierDetection.run(data);
        List<ScopeOutlier> scopeOutliers = subSpaceOutlierDetection.getScopeOutliers();
        long subSpaceOutlierDetectionTime = sw.elapsed(TimeUnit.MILLISECONDS);
       
        sw.stop();
        log.debug("...ended subSpace outlier detection (time: {}ms)!", subSpaceOutlierDetectionTime);
        sw.reset();
        
        
        
        log.debug("Starting finding explanations for every subspace outlier...");
        sw.start();
        
         
        for(ScopeOutlier scopeOutlier: scopeOutliers){
    		log.info("Scope Outlier: {}" , scopeOutlier.print(encoder));
        }
        
    	
        //the following is for finding explanations for each scoped outlier
        boolean findingExplanations = false;
        if(findingExplanations == false)
        	return;
        
    	for(ScopeOutlier scopeOutlier: scopeOutliers){
    		Stopwatch sw2 = Stopwatch.createUnstarted();
    		
    		
    		List<Integer> scopingDimensions = scopeOutlier.getScopingDimensions();
        	List<Integer> metricDimensions = scopeOutlier.getMetricDimensions();
        	
        	//This is a hack, to use existing code for scoping outlier explanation
        	//the explanation data uses the rest of the categorical attributes (excluding scoping and metric attributes)
        	//as explanation attributes
        	List<Datum> explanationData = new ArrayList<Datum>();
        	for(int i = 0; i < data.size(); i++){
        		HashSet<Integer> categoricalToKeep = new HashSet<Integer>();
        		HashSet<Integer> metricToKeep = new HashSet<Integer>();
        		for(int k = 0; k < categoricalAttributes.size(); k++){
        			if(scopingDimensions.contains(k) || metricDimensions.contains(k)){
        				continue;
        			}else{
        				categoricalToKeep.add(k);
        			}
        		}
        		Datum d = new Datum(data.get(i), categoricalToKeep, metricToKeep);
        		explanationData.add(d);
        		
        	}
    		
    		
    		sw2.start();
    		OutlierDetector.BatchResult or = subSpaceOutlierDetection.getBatchResult(explanationData, scopeOutlier);
    		FPGrowthEmerging fpg = new FPGrowthEmerging();
    		List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(or.getInliers(),
                                                                            or.getOutliers(),
                                                                            MIN_SUPPORT,
                                                                            MIN_INLIER_RATIO,
                                                                            encoder);
            sw2.stop();
           
            long summarizationTime = sw2.elapsed(TimeUnit.MILLISECONDS);
    		AnalysisResult result =  new AnalysisResult(or.getOutliers().size(), or.getInliers().size(), loadTime, subSpaceOutlierDetectionTime, summarizationTime, isr);
    		find_top_explanations(result);
            log.info("Result: {}", result.prettyPrint());
    		
    		
    		sw2.reset();
        
        }
        
        long subSpaceOutlierExplanationTime = sw.elapsed(TimeUnit.MILLISECONDS);
       
        sw.stop();
        log.debug("...ended subSpace outlier detection (time: {}ms)!", subSpaceOutlierExplanationTime);
        sw.reset();

    }
    
    /**
     * Finding explanations, either support or ratio are in top-10
     * @param result
     */
    private void find_top_explanations(AnalysisResult result){
    	if(result.getItemSets().size() > 10) {
            log.warn("Very large result set! {}; truncating to 10", result.getItemSets().size());
            result.setItemSets(result.getItemSets().subList(0, 10));
        }
    }
    
    
   
}

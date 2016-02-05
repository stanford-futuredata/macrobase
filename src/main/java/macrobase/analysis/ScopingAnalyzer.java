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

    public void analyze(SQLLoader loader,
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
        List<SubSpaceOutlier> scopeOutliers = subSpaceOutlierDetection.run(data);
    }
    
    
   
}

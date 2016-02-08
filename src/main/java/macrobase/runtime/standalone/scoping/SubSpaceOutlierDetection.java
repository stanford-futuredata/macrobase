package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.runtime.standalone.scoping.SubSpaceOutlier.ScopeOutlier;

/**
 * @author xuchu
 * Algorithm for finding outliers using subspace clustering (CLIQUE algorithm)
 */
public class SubSpaceOutlierDetection extends OutlierDetector{

	private static final Logger log = LoggerFactory.getLogger(SubSpaceOutlierDetection.class);

	/**
	 * number of intervals in each dimension
	 */
	private int numIntervals;
	
	/**
	 * minimum density for a scope to be considered frequent
	 */
	private double frequentDensity;
	
	/**
	 * maximum density for a scope to be considered 
	 */
	private double outlierDensity;
	
	
	
	private DatumEncoder encoder;
	List<String> categoricalAttributes;
	List<String> numericalAttributes;
	
	
	/**
	 * This is the result of outlier detection
	 */
	private List<ScopeOutlier> scopeOutliers;
	
	public SubSpaceOutlierDetection(int numIntervals, double frequentDensity, double outlierDensity,
			DatumEncoder encoder,
			List<String> categoricalAttributes,
			List<String> numericalAttributes){
		this.numIntervals = numIntervals;
		this.frequentDensity = frequentDensity;
		this.outlierDensity = outlierDensity;
		this.encoder = encoder;
		this.categoricalAttributes = categoricalAttributes;
		this.numericalAttributes = numericalAttributes;
	}
	
	
	/**
	 * find the set of scope outliers on the data. 
	 * @param data
	 * @return
	 */
	public void run(List<Datum> data){
		List<SubSpaceOutlier> result = new ArrayList<SubSpaceOutlier>();
		
		int totalDimensions = data.get(0).getAttributes().size() + data.get(0).getMetrics().getDimension();
		log.debug("Find one-dimensional dense scopes on all attributes");
		List<SubSpace> oneDimensionDenseSubSpaces = findOneDimensionDenseScopes(data);
		
		int metricDimension = -1;
		
		for(metricDimension = 0; metricDimension < totalDimensions; metricDimension++){
			log.debug("Use metric attribute " + metricDimension );
			
			
			List<Integer> metricDimensions = new ArrayList<Integer>();
			metricDimensions.add(metricDimension);
			Collection<Unit> metricDimensionsUnits = initOneDimensionalUnits(data,metricDimension);
			
			//identify sparse units without any scoping, and remove them from consideration
			HashSet<Unit> sparseMetricDimensionsUnits = new HashSet<Unit>();
			for(Unit metricDimensionsUnit: metricDimensionsUnits){
				if(metricDimensionsUnit.isSparse(data.size(), outlierDensity)){
					sparseMetricDimensionsUnits.add(metricDimensionsUnit);
				}
			}
			metricDimensionsUnits.removeAll(sparseMetricDimensionsUnits);
			
			
			List<SubSpace> previousLevel = new ArrayList<SubSpace>();
			
			for(SubSpace ss: oneDimensionDenseSubSpaces){
				//consider subspace that doesn't contain metric dimension
				if(ss.getDimensions().contains(metricDimension))
					continue;
				
				previousLevel.add(ss);
				ss.initializeMetricDimension(metricDimensions, metricDimensionsUnits);
			}
			
			for(int level = 2; level <= totalDimensions - 1; level++){
				
				//identify scope outlier
				for(SubSpace ss: previousLevel){
					SubSpaceOutlier so = ss.identifyScopeOutlier(data.size(), outlierDensity);
					if(so == null)
						continue;
					
					//System.err.println(  so.print(encoder) );
					result.add(so);
				}
				
				List<SubSpace> denseSubSpaces = findDenseSubSpaceOneLevelUp(previousLevel,data);
				
				//level-up
				previousLevel = denseSubSpaces;
			}
			
		}
		
		
		//sort the scope outliers
		scopeOutliers = new ArrayList<ScopeOutlier>();
		for(SubSpaceOutlier subSpaceOutlier: result){
			scopeOutliers.addAll(subSpaceOutlier.getScopeOutliers());
		}
		Collections.sort(scopeOutliers,new SubSpaceOutlier.ScopeOutlierComparator());
		
	}
	
	
	
	
	
	
	public List<ScopeOutlier> getScopeOutliers(){
		return scopeOutliers;
	}
	
	
	
	
	/**
	 * Given the dense subspaces of previous level, 
	 * find next level subspaces with dense 
	 * @param denseSubSpaces
	 * @param data
	 * @return
	 */
	private List<SubSpace> findDenseSubSpaceOneLevelUp(List<SubSpace> denseSubSpaces, List<Datum> data){
		
		//sort the subspaces by their dimensions
		List<SubSpace> denseSubspacesByDimensions = new ArrayList<>(denseSubSpaces);
	    Collections.sort(denseSubspacesByDimensions, new SubSpace.DimensionComparator());

	    //find out dense candidate subspaces 
	    List<SubSpace> result = new ArrayList<SubSpace>();
		
	    for(int i = 0; i < denseSubspacesByDimensions.size(); i++ ){
	    	for(int j = i +1; j < denseSubspacesByDimensions.size(); j++){
	    		SubSpace s1 = denseSubspacesByDimensions.get(i);
	    		SubSpace s2 = denseSubspacesByDimensions.get(j);
	    		SubSpace joined = s1.join(s2, data.size(), frequentDensity);
	    		
	    		if(joined != null){
	    			result.add(joined);
	    		}
	    	}
	    }
	    
	    
		return result;
	}
	

	
	
	
	/**
	 * Find one dimensional subspace with dense scopes
	 * @param data
	 * @return
	 */
	private List<SubSpace> findOneDimensionDenseScopes(List<Datum> data){
		
		Collection<Unit> oneDimensionalUnits = initOneDimensionalUnits(data);
		
		//find out the dense one dimensional units
		Map<Integer,List<Unit>> dimension2OneDimensionalDenseUnits = new HashMap<Integer, List<Unit>>();
		for(Unit unit: oneDimensionalUnits){
			if(unit.isDense(data.size(), frequentDensity)){
				
				int dimension = unit.getDimensions().get(0);
				if(!dimension2OneDimensionalDenseUnits.containsKey(dimension))
					dimension2OneDimensionalDenseUnits.put(dimension, new ArrayList<Unit>());
				
				dimension2OneDimensionalDenseUnits.get(dimension).add(unit);
			}
		}
		
		//create subspaces
		List<SubSpace> subspaces = new ArrayList<SubSpace>();
		for(int dimension: dimension2OneDimensionalDenseUnits.keySet()){
			SubSpace ss = new SubSpace(dimension);
			for(Unit denseUnit: dimension2OneDimensionalDenseUnits.get(dimension)){
				ss.addDenseUnit(denseUnit);
			}
			subspaces.add(ss);
		}
		
		
		return subspaces;
	}
	
	/**
	 * Initialize one dimensional units
	 * @param data
	 * @return
	 */
	private Collection<Unit> initOneDimensionalUnits(List<Datum> data){
		
		Collection<Unit> result = new ArrayList<Unit>();
		
		int categoricalDimensions = data.get(0).getAttributes().size();
		int numericalDimensions = data.get(0).getMetrics().getDimension();
		int totalDimensions = categoricalDimensions +numericalDimensions ;
		
		for(int dimension = 0; dimension < totalDimensions; dimension++){
			result.addAll( initOneDimensionalUnits(data,dimension) );
		}
		return result;
	}
	
	private Collection<Unit> initOneDimensionalUnits(List<Datum> data, int dimension){
		int categoricalDimensions = data.get(0).getAttributes().size();
		
		
		Collection<Unit> result = new ArrayList<Unit>();
		
		if(dimension < categoricalDimensions){
			HashSet<Integer> distinctValues = new HashSet<Integer>();
			for(Datum datum: data){
				distinctValues.add(datum.getAttributes().get(dimension));
			}
			for(Integer value: distinctValues){
				Interval interval = new Interval(dimension,categoricalAttributes.get(dimension),value);
				Unit unit = new Unit(dimension, interval);
				result.add(unit);
			}
		}else{
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			//find out the min, max
			for(Datum datum: data){
				double value = datum.getMetrics().getEntry(dimension - categoricalDimensions );
				if(value > max){
					max = value;
				}
				if(value < min){
					min = value;
				}
			}
			// divide the interval into numIntervals
			double step = (max - min) / numIntervals;
			double start = min;
			for(int i = 0; i < numIntervals; i++){
				if(i != numIntervals - 1){
					Interval interval = new Interval(dimension,numericalAttributes.get(dimension - categoricalDimensions), start, start + step);
					start += step;
					Unit unit = new Unit(dimension, interval);
					result.add(unit);
				}else{
					//make the max a little bit larger
					Interval interval = new Interval(dimension, numericalAttributes.get(dimension - categoricalDimensions),start, max + 0.000001);
					Unit unit = new Unit(dimension, interval);
					result.add(unit);
				}
				
			}
		}
		
		
		//add the support tids of units
		for(int t = 0; t < data.size(); t++){
			Datum datum = data.get(t);
			for(Unit unit: result){
				if(unit.containDatum(datum)){
					unit.addTIDs(t);
				}
			}
		}
				
				
		return result;
	}

	
	public OutlierDetector.BatchResult getBatchResult(
			List<Datum> explanationData,
			ScopeOutlier scopeOutlier){
		
		OutlierDetector.BatchResult or;
		
		List<DatumWithScore> inliners = new ArrayList<DatumWithScore>();
		List<DatumWithScore> outliers = new ArrayList<DatumWithScore>();
		
		List<Integer> outlierTIDs = scopeOutlier.getOutlierTIDs();
		List<Integer> inlinerTIDs = scopeOutlier.getInlinerTIDs();
		
		for(int i = 0; i < explanationData.size(); i++){
			DatumWithScore datumWithScore = new DatumWithScore(explanationData.get(i),0);
			if(outlierTIDs.contains(i)){
				outliers.add(datumWithScore);
			}else if(inlinerTIDs.contains(i)){
				inliners.add(datumWithScore);
			}else{
				
			}
		}
		or = new OutlierDetector.BatchResult(inliners,outliers);
		
		return or;
	}
	

	@Override
	public void train(List<Datum> data) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public double score(Datum datum) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public double getZScoreEquivalent(double zscore) {
		// TODO Auto-generated method stub
		return 0;
	}
}

package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;

/**
 * @author xuchu
 * Algorithm for finding outliers using subspace clustering (CLIQUE algorithm)
 */
public class SubSpaceOutlierDetection {

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
	 * find the set of scope outliers on the data
	 * @param data
	 * @return
	 */
	public List<SubSpaceOutlier> run(List<Datum> data){
		
		List<SubSpaceOutlier> allOutliers = new ArrayList<SubSpaceOutlier>();
		
		log.debug("Find one-dimensional dense scopes");
		
		int totalDimensions = data.get(0).getAttributes().size() + data.get(0).getMetrics().getDimension();
		List<SubSpace> oneDimensionDenseSubSpaces = findOneDimensionDenseScopes(data);
		
		List<SubSpace> previousLevel = oneDimensionDenseSubSpaces;
	
		for(int level = 2; level <= totalDimensions; level++){
			
			log.debug("Find " + level + "  dimensional outlier scopes");
			List<SubSpaceOutlier> outlierSubSpaces = findOutlierSubSpaceOneLevelUp(previousLevel,data);
			allOutliers.addAll(outlierSubSpaces);
			
			for(SubSpaceOutlier outlier: outlierSubSpaces){
				System.err.println(outlier.print(encoder));
			}
			

			//test only three levels for now
			if(level == 3)
				break;
			
			log.debug("Find " + level + "  dimensional dense scopes");
			List<SubSpace> denseSubSpaces = findDenseSubSpaceOneLevelUp(previousLevel,data);
		
			//level-up
			previousLevel = denseSubSpaces;
			
		}
		
		
		//for(SubSpaceOutlier outlier: allOutliers){
		//	System.err.println(outlier.print(encoder));
		//}
		
		
		return allOutliers;
		
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
	 * Given the dense subspaces of previous level, 
	 * find next level subspaces with outliers 
	 * @param denseSubSpaces
	 * @param data
	 * @return
	 */
	private List<SubSpaceOutlier> findOutlierSubSpaceOneLevelUp(List<SubSpace> denseSubSpaces, List<Datum> data){
		
		//all outlier subspaces
		List<SubSpaceOutlier> result = new ArrayList<SubSpaceOutlier>();
		
		
		
		//sort the subspaces by their dimensions
		List<SubSpace> denseSubspacesByDimensions = new ArrayList<>(denseSubSpaces);
	    Collections.sort(denseSubspacesByDimensions, new SubSpace.DimensionComparator());

	    for(int i = 0; i < denseSubspacesByDimensions.size(); i++ ){
	    	for(int j = i +1; j < denseSubspacesByDimensions.size(); j++){
	    		
	    		SubSpace s1 = denseSubspacesByDimensions.get(i);
	    		SubSpace s2 = denseSubspacesByDimensions.get(j);
	    		
	    		List<Integer> newDimensions = s1.joinedDimensions(s2);
	    		if(newDimensions == null)
	    			continue;
	    		
	    		SubSpaceOutlier subSpaceOutlier = null; 
	    		
	    		List<Unit> denseUnits1 = s1.getDenseUnits();
	    		List<Unit> denseUnits2 = s2.getDenseUnits();
	    		
	    		for(Unit u1: denseUnits1){
	    			for(Unit u2: denseUnits2){
	    				Unit newUnit = u1.join(u2);
	    				if(newUnit == null)
	    					continue;
	    				if(newUnit.isSparse(data.size(), outlierDensity)){
	    					//This is a Sparse new unit, check if every sub-unit is dense
	    					if(checkSubUnitDensity(newUnit,denseSubSpaces)){
	    						if(subSpaceOutlier == null)
	    							subSpaceOutlier = new SubSpaceOutlier(newDimensions);
	    						subSpaceOutlier.addOutlierUnit(newUnit);
	    					}
	    				}
	    			}
	    		}
	    		
	    		if(subSpaceOutlier != null)
	    			result.add(subSpaceOutlier);
	    		
	    	}
	    }
		
		return result;
	}
	
	/**
	 * Check if all sub-units of this newUnit are dense, return true if yes
	 * @param newUnit
	 * @param denseSubSpaces
	 * @return
	 */
	private boolean checkSubUnitDensity(Unit newUnit, List<SubSpace> denseSubSpaces){
		
		List<Unit> subUnits = newUnit.getImmediateSubUnits();
		
		for(Unit subUnit: subUnits){
			boolean denseSubUnit = false;
			for(SubSpace subSpace: denseSubSpaces){
				if(!subUnit.getDimensions().equals(subSpace.getDimensions())){
					continue;
				}
				if(subSpace.getDenseUnits().contains(subUnit)){
					denseSubUnit = true;
					break;
				}
			}
			if(denseSubUnit == false)
				return false;
		}
		
		return true;
	}
	
	
	
	/**
	 * Find one dimensional subspace with dense scopes
	 * @param data
	 * @return
	 */
	private List<SubSpace> findOneDimensionDenseScopes(List<Datum> data){
		
		Collection<Unit> oneDimensionalUnits = initOneDimensionalUnits(data);
		//add the support tids of units
		for(int t = 0; t < data.size(); t++){
			Datum datum = data.get(t);
			for(Unit unit: oneDimensionalUnits){
				if(unit.containDatum(datum)){
					unit.addTIDs(t);
				}
			}
		}
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
		
		int dimension = 0;
		
		//categorical dimensions, one distinct value per interval
		for(dimension = 0; dimension < categoricalDimensions; dimension++){
			HashSet<Integer> distinctValues = new HashSet<Integer>();
			for(Datum datum: data){
				distinctValues.add(datum.getAttributes().get(dimension));
			}
			for(Integer value: distinctValues){
				Interval interval = new Interval(dimension,categoricalAttributes.get(dimension),value);
				Unit unit = new Unit(dimension, interval);
				result.add(unit);
			}
			
		}
		
		//numerical dimensions
		for(; dimension < totalDimensions; dimension++){
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
		return result;
	}
}

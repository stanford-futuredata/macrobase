package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author xuchu
 * Represents a subspace in the full dimensional space
 */

public class SubSpace {

	
	/**
	 * A list of ordered dimensions this subspace has, these are scoping dimensions;
	 */
	List<Integer> dimensions;
	
	
	/**
	 * A list of dense units this subspace has on the scoping dimensions
	 */
	List<Unit> denseUnits;
	
	
	/**
	 * The metric dimensions
	 */
	List<Integer> metricDimensions;
	
	
	/**
	 * The following fields are specific to an outlier detection method, right now it is count-based
	 */
	Map<Unit,HashMap<Unit,List<Integer>>>  denseUnit2metricUnit2MergedTIDs = new HashMap<Unit,HashMap<Unit,List<Integer>>>();
	
	
	/**
	 * Create a one dimensional subspace
	 * @param dimension
	 */
	public SubSpace(int dimension){
		dimensions = new ArrayList<Integer>();
		dimensions.add(dimension);
	}
	
	public SubSpace(List<Integer> dimensions){
		this.dimensions = dimensions;
	}
	
	public List<Integer> getDimensions(){
		return dimensions;
	}
	
	/**
	 * Return 
	 * @param other
	 * @param total
	 * @param tau
	 * @return
	 */
	public SubSpace join(SubSpace other,int total, double tau){
		
		List<Integer> newDimensions = joinedDimensions(other);
		
		if(newDimensions == null)
			return null;
		
		//now start to join the dense units
		SubSpace result = new SubSpace(newDimensions);
		for(Unit u1: getDenseUnits()){
			for(Unit u2: other.getDenseUnits()){
				Unit newUnit = u1.join(u2);
				if(newUnit == null)
					continue;
				if(newUnit.isDense(total, tau)){
					result.addDenseUnit(newUnit);
				}
				
				
				//join the metric unit part
				result.metricDimensions = metricDimensions;
				HashMap<Unit,List<Integer>> metricUnit2MergedTIDs = joinMetricUnit(denseUnit2metricUnit2MergedTIDs.get(u1),other.denseUnit2metricUnit2MergedTIDs.get(u2));
				result.denseUnit2metricUnit2MergedTIDs.put(newUnit, metricUnit2MergedTIDs);
			}
		}
		
		//only interested in SubSpace that contains dense units
		if(result.denseUnits == null || result.denseUnits.size() == 0)
			return null;
		return result;
		
	}
	
	/**
	 * Joins this subspace with the specified subspace. The join is only
	 * successful if both subspaces have the first k-1 dimensions in common (where
	 * k is the number of dimensions).
	 * 
	 * Return null is not successful
	 * @param other
	 * @return
	 */
	public List<Integer> joinedDimensions(SubSpace other){
		//check the dimensions first
		if(dimensions.size() != other.dimensions.size())
			return null;
		
		List<Integer> newDimensions = new ArrayList<Integer>();
		
		for(int i = 0; i < dimensions.size() - 1; i++){
			if(dimensions.get(i) != other.dimensions.get(i))
				return null;
			else
				newDimensions.add(dimensions.get(i));
		}
		
		int lastDimension1 = dimensions.get(dimensions.size() - 1);
		int lastDimension2 = other.dimensions.get(dimensions.size() - 1);
		if(lastDimension1 == lastDimension2){
			return null;
		}
		else if(lastDimension1 < lastDimension2){
			newDimensions.add(lastDimension1);
			newDimensions.add(lastDimension2);
		}else{
			newDimensions.add(lastDimension2);
			newDimensions.add(lastDimension1);
		}
		return newDimensions;
	}
	
	
	public List<Unit> getDenseUnits(){
		return denseUnits;
	}
	
	/**
	 * Add dense unit, only add if the unit has the same dimensions as this subspace
	 * @param unit
	 */
	public void addDenseUnit(Unit unit){
		if(denseUnits == null)
			denseUnits = new ArrayList<Unit>();
		
		if(dimensions.size() !=  unit.getDimensions().size())
			return;
		for(int i = 0; i < dimensions.size(); i++){
			if(dimensions.get(i) != unit.getDimensions().get(i))
				return;
		}
		
		denseUnits.add(unit);
	}
	
	
	
	/**
	 * Compare the subspace based on the dimensions
	 * @author xuchu
	 *
	 */
	public static class DimensionComparator implements Comparator<SubSpace> {
		@Override
		public int compare(SubSpace s1, SubSpace s2) {
			if (s1 == s2) {
				return 0;
			}

			if (s1.dimensions == null && s2.dimensions != null) {
				return -1;
			}

			if (s1.dimensions != null && s2.dimensions == null) {
				return 1;
			}

			int compare = s1.dimensions.size() - s2.dimensions.size();
			if (compare != 0) {
				return compare;
			}

			for (int i = 0; i < s1.dimensions.size(); i++) {
				int d1 = s1.dimensions.get(i);
				int d2 = s2.dimensions.get(i);
				if (d1 != d2) {
					return d1 - d2;
				}
			}
			return 0;

		}
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("Dense Units are: ");
		for(Unit unit: denseUnits){
			sb.append(unit.toString());
		}
		return sb.toString();
	}
	
	
	//The following methods are outlier detection methods specific
	
	/**
	 * Initialize metric dimension related fields
	 * @param metricDimension
	 * @param metricInterval2TIDs
	 */
	public void initializeMetricDimension(List<Integer> metricDimensions, Collection<Unit> metricUnits){
		this.metricDimensions = metricDimensions;
			
		if(denseUnits == null)
			return;
		
		for(Unit denseUnit: denseUnits){
			List<Integer> tids1 = denseUnit.getTIDs();
			
			HashMap<Unit,List<Integer>> metricUnit2MergedTIDs = new HashMap<Unit,List<Integer>>();
			
			for(Unit metricUnit: metricUnits){
				List<Integer> tids2 = metricUnit.getTIDs();
				
				List<Integer> mergedTIDs = mergeTIDs(tids1,tids2);
				
				if(mergedTIDs.size() == 0)
					continue;
				
				metricUnit2MergedTIDs.put(metricUnit, mergedTIDs);
				
			}
			
			denseUnit2metricUnit2MergedTIDs.put(denseUnit, metricUnit2MergedTIDs);
		}
	}
	
	/**
	 * Join the metric unit, when joining two dense units
	 * @param metricUnit2MergedTIDs1
	 * @param metricUnit2MergedTIDs2
	 * @return
	 */
	private HashMap<Unit,List<Integer>> joinMetricUnit(HashMap<Unit,List<Integer>> metricUnit2MergedTIDs1, HashMap<Unit,List<Integer>> metricUnit2MergedTIDs2){
		HashMap<Unit,List<Integer>> result = new HashMap<Unit, List<Integer>>();
	
		for(Unit metricUnit: metricUnit2MergedTIDs1.keySet()){
			if(!metricUnit2MergedTIDs2.containsKey(metricUnit))
				continue;
			
			List<Integer> tids1 = metricUnit2MergedTIDs1.get(metricUnit);
			List<Integer> tids2 = metricUnit2MergedTIDs2.get(metricUnit);
			
			List<Integer> mergedTIDs = mergeTIDs(tids1,tids2);
			if(mergedTIDs.size() != 0){
				result.put(metricUnit, mergedTIDs);
			}
		}
		return result;
	}
	
	
	/**
	 * Sorted merge
	 * @param tids1
	 * @param tids2
	 * @return
	 */
	private List<Integer> mergeTIDs(List<Integer> tids1, List<Integer> tids2){
		List<Integer> result = new ArrayList<Integer>();
		//merge two sorted tids
		int index1 = 0;
		int index2 = 0;
		while(index1 < tids1.size() && index2 < tids2.size()){
			int tid1 = tids1.get(index1);
			int tid2 = tids2.get(index2);
			if(tid1 == tid2){
				result.add(tid1);
				index1++;
				index2++;
			}else if(tid1 < tid2){
				index1++;
			}else{
				index2++;
			}
		}
		return result;
	}
	
	
	/**
	 * Identify the scope outlier, whose support is less than tau
	 * @param tau
	 * @return
	 */
	public SubSpaceOutlier identifyScopeOutlier(int total, double tau){
		SubSpaceOutlier scopeOutlier = new SubSpaceOutlier(dimensions,metricDimensions);
		
		for(Unit denseUnit: denseUnits){
			
			HashSet<Unit> outlierUnits = new HashSet<Unit>();
			
			for(Unit metricUnit: denseUnit2metricUnit2MergedTIDs.get(denseUnit).keySet()){
				
				List<Integer> tids = denseUnit2metricUnit2MergedTIDs.get(denseUnit).get(metricUnit);
				
				double density = (double) tids.size() / total;
				
				if(tids.size() != 0 && density <= tau){
					scopeOutlier.addScopeOutlier(denseUnit, metricUnit);
					
					outlierUnits.add(metricUnit);
				}
				
			}
			
			//remove outlier units
			for(Unit outlierUnit: outlierUnits){
				denseUnit2metricUnit2MergedTIDs.get(denseUnit).remove(outlierUnit);
			}
		}
		
		if(scopeOutlier.hasOutliers())
			return scopeOutlier;
		else
			return null;
	}
	
}

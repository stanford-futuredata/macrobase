package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author xuchu
 * Represents a subspace in the full dimensional space
 */

public class SubSpace {

	
	/**
	 * A list of ordered dimensions this subspace has
	 */
	List<Integer> dimensions;
	
	
	/**
	 * A list of dense units this subspace has
	 */
	List<Unit> denseUnits;
	
	
	
	
	
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
}

package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
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
	
	/**
	 * Joins this subspace with the specified subspace. The join is only
	 * successful if both subspaces have the first k-1 dimensions in common (where
	 * k is the number of dimensions) and the last dimension of this subspace is
	 * less than the last dimension of the specified subspace.
	 * @param other
	 * @return
	 */
	public SubSpace join(SubSpace other){
		return null;
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
	
}

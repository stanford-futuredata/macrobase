package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.List;

public class SubSpaceOutlier {
	/**
	 * A list of ordered dimensions this subspace has
	 */
	List<Integer> dimensions;
	
	
	/**
	 * A list of outlier units this subspace has
	 * A unit is an outlier if all subunits are dense, and this unit is sparse
	 */
	
	List<Unit> outlierUnits;
	
	
	public SubSpaceOutlier(List<Integer> dimensions){
		this.dimensions = dimensions;
	}
	
	
	public List<Unit> getOutlierUnits(){
		return outlierUnits;
	}
	
	/**
	 * Add outlier unit, only add if the unit has the same dimensions as this subspace
	 * @param unit
	 */
	public void addOutlierUnit(Unit unit){
		
		if(outlierUnits == null)
			outlierUnits = new ArrayList<Unit>();
		if(dimensions.size() !=  unit.getDimensions().size())
			return;
		
		for(int i = 0; i < dimensions.size(); i++){
			if(dimensions.get(i) != unit.getDimensions().get(i))
				return;
		}
		
		outlierUnits.add(unit);
		
	}
}

package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import macrobase.ingest.DatumEncoder;
import macrobase.ingest.result.ColumnValue;

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
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("Outlier Units are: ");
		for(Unit unit: outlierUnits){
			sb.append(unit.toString());
		}
		return sb.toString();
	}
	
	/**
	 * Provide a human-readable print of the outlier
	 * @param encoder
	 * @return
	 */
	public String print(DatumEncoder encoder){
		
		StringBuilder sb = new StringBuilder();
		sb.append("Outlier Units are: ");
		for(Unit unit: outlierUnits){
			sb.append(unit.print(encoder));
		}
		return sb.toString();
		
	}
	
}

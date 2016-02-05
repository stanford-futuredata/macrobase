package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import macrobase.ingest.DatumEncoder;
import macrobase.ingest.result.ColumnValue;

public class SubSpaceOutlier {
	/**
	 * A list of ordered scoping dimensions this subspace has
	 */
	List<Integer> scopingDimensions;
	
	/**
	 * The metric dimensions
	 */
	List<Integer> metricDimensions;
	
	
	List<ScopeOutlier> scopeOutliers = new ArrayList<ScopeOutlier>();
	
	public SubSpaceOutlier(List<Integer> scopingDimensions, List<Integer> metricDimensions){
		this.scopingDimensions = scopingDimensions;
		this.metricDimensions = metricDimensions;
	
	}
	
	/**
	 * Add one outlier, consisting of scope unit and outlier unit
	 * @param scopeUnit
	 * @param outlierUnit
	 */
	public void addScopeOutlier(Unit scopeUnit, Unit outlierUnit){
		ScopeOutlier so = new ScopeOutlier(scopeUnit, outlierUnit);
		scopeOutliers.add(so);
	}
	
	public boolean hasOutliers(){
		if(scopeOutliers.isEmpty())
			return false;
		
		return true;
	}
	
	/**
	 * Provide a human-readable print of the outlier
	 * @param encoder
	 * @return
	 */
	public String print(DatumEncoder encoder){
		
		StringBuilder sb = new StringBuilder();
		for(ScopeOutlier so: scopeOutliers){
			sb.append(so.print(encoder)	);
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	
	class ScopeOutlier{
		Unit scopeUnit;
		Unit outlierUnit;
		
		public ScopeOutlier(Unit scopeUnit, Unit outlierUnit){
			this.scopeUnit = scopeUnit;
			this.outlierUnit = outlierUnit;
		}
		
		public String print(DatumEncoder encoder){
			return "Scope: " + scopeUnit.print(encoder) + " Outlier: " + outlierUnit.print(encoder);
		}
	}
}

package macrobase.runtime.standalone.scoping;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import macrobase.ingest.DatumEncoder;

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
	 * Get the scoping dimensions
	 * @return
	 */
	public List<Integer> getScopingDimensions(){
		return scopingDimensions;
	}
	
	/**
	 * Get the metric dimensions
	 * @return
	 */
	public List<Integer> getMetricDimensions(){
		return metricDimensions;
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
	
	public List<ScopeOutlier> getScopeOutliers(){
		return scopeOutliers;
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
	
	
	public class ScopeOutlier{
		Unit scopeUnit;
		Unit outlierUnit;
		
		List<Integer> inlinerTIDs;
		List<Integer> outlierTIDs;
		double score;
		
		public ScopeOutlier(Unit scopeUnit, Unit outlierUnit){
			this.scopeUnit = scopeUnit;
			this.outlierUnit = outlierUnit;
			
			inlinerTIDs = new ArrayList<Integer>();
			outlierTIDs = new ArrayList<Integer>();
			
			for(Integer tid: scopeUnit.getTIDs()){
				if(outlierUnit.getTIDs().contains(tid)){
					outlierTIDs.add(tid);
				}else{
					inlinerTIDs.add(tid);
				}
			}
			//the smaller, the better
			score = (double)outlierTIDs.size() / inlinerTIDs.size();
			
			
		}
		
		public double getScore(){
			return score;
		}
		
		public List<Integer> getInlinerTIDs(){
			return inlinerTIDs;
		}
		
		public List<Integer> getOutlierTIDs(){
			return outlierTIDs;
		}
		
		public List<Integer> getScopingDimensions(){
			return scopingDimensions;
		}
	
		public List<Integer> getMetricDimensions(){
			return metricDimensions;
		}
		public String print(DatumEncoder encoder){
			return "Scope: " + scopeUnit.print(encoder) + " Outlier: " + outlierUnit.print(encoder)
			+ " NumInliners: " + inlinerTIDs.size() + " NumOutliers: " + outlierTIDs.size();
		}
		
		
	}
	public static class ScopeOutlierComparator implements Comparator<ScopeOutlier> {

		@Override
		public int compare(ScopeOutlier o1, ScopeOutlier o2) {
			if(o1.getScore() > o2.getScore())
				return 1;
			else if(o1.getScore() < o2.getScore())
				return -1;
			else
				return 0;
		}
	
		
	}
}

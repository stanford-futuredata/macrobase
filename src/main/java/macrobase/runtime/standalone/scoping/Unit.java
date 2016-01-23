package macrobase.runtime.standalone.scoping;

import java.util.*;

import macrobase.datamodel.Datum;

/**
 * 
 * @author xuchu
 * Represent a unit in the CLIQUE algorithm
 */



public class Unit {

	/**
	 * A set of intervals this unit contains
	 * one dimension can have at most one interval
	 */
	private SortedMap<Integer,Interval> dimension2Interval;
	
	/**
	 * A list of tuple ids that this unit contains
	 */
	private List<Integer> tids;
	
	
	/**
	 * Initialize a one dimensional unit
	 * @param dimension
	 * @param interval
	 */
	public Unit(int dimension, Interval interval){
		dimension2Interval = new TreeMap<Integer, Interval>();
		dimension2Interval.put(dimension, interval);
	}
	
	
	public List<Integer> getDimensions(){
		List<Integer> result = new ArrayList<Integer>();
		for(Integer k: dimension2Interval.keySet()){
			result.add(k);
		}
		return result;
	}
	
	/**
	 * Determine if the unit contains the tuple
	 * @param datum
	 * @return
	 */
	public boolean containDatum(Datum datum){
		
		int categoricalDimensions = datum.getAttributes().size();
		int numericalDimensions = datum.getMetrics().getDimension();
		int totalDimensions = categoricalDimensions +numericalDimensions ;
	
		
		for(Integer k: dimension2Interval.keySet()){
			if( k >=0 && k < categoricalDimensions){
				int value = datum.getAttributes().get(k);
				if(!dimension2Interval.get(k).contains(value))
					return false;
			}else if( k >= categoricalDimensions && k < totalDimensions){
				double value = datum.getMetrics().getEntry(k - categoricalDimensions);
				if(!dimension2Interval.get(k).contains(value))
					return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Add a tid, whose datum is contained in this unit
	 * @param tid
	 */
	public void addTIDs(int tid){
		if(tids == null)
			tids = new ArrayList<Integer>();
		
		tids.add(tid);
	}
	
	/**
	 * Is this a dense unit
	 * @param total
	 * @param tau
	 * @return
	 */
	public boolean isDense(int total, double tau){
		double density = (double) tids.size() / total;
		if(density > tau)
			return true;
		else
			return false;
	}
}

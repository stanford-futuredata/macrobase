package macrobase.runtime.standalone.scoping;

import java.util.*;

import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;

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
		
		tids = new ArrayList<Integer>();
	}
	
	
	public Unit(SortedMap<Integer,Interval> dimension2Interval){
		this.dimension2Interval = dimension2Interval;
		
		tids = new ArrayList<Integer>();
	}
	
	public List<Integer> getDimensions(){
		List<Integer> result = new ArrayList<Integer>();
		for(Integer k: dimension2Interval.keySet()){
			result.add(k);
		}
		return result;
	}
	
	public List<Integer> getTIDs(){
		return tids;
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
	
	/**
	 * Is this a sparse unit
	 * @param total
	 * @param tau
	 * @return
	 */
	public boolean isSparse(int total, double tau){
		double density = (double) tids.size() / total;
		if(density <= tau)
			return true;
		else
			return false;
	}
	
	/**
	 * Join this unit with other unit, can only be joined if the first (k-1) dimensions
	 * have the same interval, and the last dimension has different interval
	 * 
	 * If not, return null
	 * 
	 * 
	 * @param other
	 * @return
	 */
	public Unit join(Unit other){
		
		SortedMap<Integer,Interval> newDimension2Interval = new TreeMap<Integer,Interval>();
		
		
		List<Integer> dimensions1 = getDimensions();
		List<Integer> dimensions2 = other.getDimensions();
		if(dimensions1.size() != dimensions2.size())
			return null;
		
		for(int i = 0; i < dimensions1.size(); i++){
			int dimension1 = dimensions1.get(i);
			int dimension2 = dimensions2.get(i);
			Interval interval1 = dimension2Interval.get(dimension1);
			Interval interval2 = other.dimension2Interval.get(dimension2);
		
			if(i !=dimensions1.size() - 1 ){
				if(dimension1 != dimension2)
					return null;
					if(interval1 != interval2)
					return null;
				newDimension2Interval.put(dimension1, interval1);
			}else{
					newDimension2Interval.put(dimension1, interval1);
				newDimension2Interval.put(dimension2, interval2);
			}
			
		}
	
		Unit newUnit = new Unit(newDimension2Interval);
		
		//merge two sorted tids
		int index1 = 0;
		int index2 = 0;
		while(index1 < tids.size() && index2 < other.tids.size()){
			int tid1 = tids.get(index1);
			int tid2 = other.tids.get(index2);
			if(tid1 == tid2){
				newUnit.addTIDs(tid1);
				index1++;
				index2++;
			}else if(tid1 < tid2){
				index1++;
			}else{
				index2++;
			}
		}
		
		return newUnit;
	}
	
	
	/**
	 * Generate immediate sub-units of this unit by removing one-dimension
	 * @return
	 */
	public List<Unit> getImmediateSubUnits(){
		List<Unit> result = new ArrayList<Unit>();
		
		for(Integer dimension: dimension2Interval.keySet()){
			SortedMap<Integer,Interval> newDimension2Interval = new TreeMap<Integer,Interval>(dimension2Interval);
			newDimension2Interval.remove(dimension);
			Unit subUnit = new Unit(newDimension2Interval);
			result.add(subUnit);
		}
		
		return result;
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dimension2Interval == null) ? 0 : dimension2Interval.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Unit other = (Unit) obj;
		
		List<Integer> dimensions1 = getDimensions();
		List<Integer> dimensions2 = other.getDimensions();
		if(dimensions1.size() != dimensions2.size())
			return false;
		
		for(int i = 0; i < dimensions1.size(); i++){
			int d1 = dimensions1.get(i);
			int d2 = dimensions2.get(i);
			if(d1 != d2)
				return false;
			Interval interval1 = dimension2Interval.get(d1);
			Interval interval2 = other.dimension2Interval.get(d2);
			if(interval1 != interval2)
				return false;
		}
		return true;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		for(Integer dimension: dimension2Interval.keySet()){
			sb.append(dimension2Interval.get(dimension).toString());
		}
		sb.append(" }");
		return sb.toString();
	}
	
	/**
	 * Provide a human-readable print of the Unit
	 * @param encoder
	 * @return
	 */
	public String print(DatumEncoder encoder){
		
		StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		for(Integer dimension: dimension2Interval.keySet()){
			sb.append(dimension2Interval.get(dimension).print(encoder));
		}
		sb.append(" }");
		return sb.toString();
		
	}
	
}

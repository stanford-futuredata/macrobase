package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;


public class Context {
    private static final Logger log = LoggerFactory.getLogger(Context.class);

    
    /**
	 * A set of intervals this context contains
	 * one dimension can have at most one interval
	 */
	private SortedMap<Integer,Interval> dimension2Interval;
	
	
	/**
	 * A list of tuple ids that this unit contains
	 */
	private List<Integer> tids;
	
	/**
	 * Initialize a one dimensional context
	 * @param dimension
	 * @param interval
	 */
	public Context(int dimension, Interval interval){
		dimension2Interval = new TreeMap<Integer, Interval>();
		dimension2Interval.put(dimension, interval);
		
		tids = new ArrayList<Integer>();
	}
	
	public Context(SortedMap<Integer, Interval> newDimension2Interval) {
		this.dimension2Interval = newDimension2Interval;
		
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
	public void addTID(int tid){
		tids.add(tid);
	}
	public boolean removeTID(int tid){
		return tids.remove(new Integer(tid));
	}
	
	/**
	 * 
	 * @param total is the total number of tuples, tau is the minimum threshold
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
	 * Determine if the unit contains the tuple
	 * @param datum
	 * @return
	 */
	public boolean containDatum(Datum datum){
		
		int discreteDimensions = datum.getContextualDiscreteAttributes().size();
		int doubleDimensions = datum.getContextualDoubleAttributes().getDimension();
		int totalDimensions = discreteDimensions + doubleDimensions ;
	
		
		for(Integer k: dimension2Interval.keySet()){
			if( k >=0 && k < discreteDimensions){
				int value = datum.getContextualDiscreteAttributes().get(k);
				if(!dimension2Interval.get(k).contains(value))
					return false;
			}else if( k >= discreteDimensions && k < totalDimensions){
				double value = datum.getContextualDoubleAttributes().getEntry(k - discreteDimensions);
				if(!dimension2Interval.get(k).contains(value))
					return false;
			}
		}
		
		return true;
	}
	/**
	 * Join this Context with other Context, can only be joined if the first (k-1) dimensions
	 * have the same interval, and the last dimension has different interval
	 * 
	 * can be joined only if the size of the context is at least minSize
	 * 
	 * 
	 * @param other
	 * @return
	 */
	public Context join(Context other, int minSize){
		
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
	
		if(ContextPruning.densityPruning(this, other, minSize))
			return null;
		
		Context newUnit = new Context(newDimension2Interval);
		//merge two sorted tids
		int index1 = 0;
		int index2 = 0;
		while(index1 < tids.size() && index2 < other.tids.size()){
			int tid1 = tids.get(index1);
			int tid2 = other.tids.get(index2);
			if(tid1 == tid2){
				newUnit.addTID(tid1);
				index1++;
				index2++;
			}else if(tid1 < tid2){
				index1++;
			}else{
				index2++;
			}
		}
		
		if(newUnit.getTIDs().size() >= minSize)
			return newUnit;
		else
			return null;
	}
	
	public String print(DatumEncoder encoder){
		StringBuilder sb = new StringBuilder();
		for(int d: dimension2Interval.keySet()){
			sb.append( dimension2Interval.get(d).print(encoder) + " ");
		}
		return sb.toString();
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(int d: dimension2Interval.keySet()){
			sb.append(d + ":" + dimension2Interval.get(d).toString() + " ");
		}
		return sb.toString();
	}
	
}

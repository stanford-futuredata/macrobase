package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;


public class Context {
    private static final Logger log = LoggerFactory.getLogger(Context.class);

    
    /**
	 * A list of ordered contextual dimensions this node represents
	 * and intervals for each dimension
	 */
	List<Integer> dimensions = new ArrayList<Integer>();
	List<Interval> intervals = new ArrayList<Interval>();;
	
	private int size = -1;
	
	
	/**
	 * Initialize a one dimensional context
	 * @param dimension
	 * @param interval
	 */
	public Context(int dimension, Interval interval){
		dimensions.add(dimension);
		intervals.add(interval);
		
	}
	
	public Context(List<Integer> dimensions, List<Interval> intervals) {
		this.dimensions = dimensions;
		this.intervals = intervals;
		
	}

	public List<Integer> getDimensions(){
		return dimensions;
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
	
		
		for(int i = 0; i < dimensions.size(); i++){
			int k = dimensions.get(i);
			if( k >=0 && k < discreteDimensions){
				int value = datum.getContextualDiscreteAttributes().get(k);
				if(!intervals.get(i).contains(value))
					return false;
			}else if( k >= discreteDimensions && k < totalDimensions){
				double value = datum.getContextualDoubleAttributes().getEntry(k - discreteDimensions);
				if(!intervals.get(i).contains(value))
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
	public Context join(Context other, List<Datum> data, double tau){
		
		List<Integer> newDimensions = new ArrayList<Integer>();
		List<Interval> newIntervals = new ArrayList<Interval>();
		
		
		List<Integer> dimensions1 = getDimensions();
		List<Integer> dimensions2 = other.getDimensions();
		if(dimensions1.size() != dimensions2.size())
			return null;
		
		for(int i = 0; i < dimensions1.size(); i++){
			int dimension1 = dimensions1.get(i);
			int dimension2 = dimensions2.get(i);
			Interval interval1 = intervals.get(i);
			Interval interval2 = other.intervals.get(i);
		
			if(i !=dimensions1.size() - 1 ){
				if(dimension1 != dimension2)
					return null;
					if(interval1 != interval2)
					return null;
					newDimensions.add(dimension1);
					newIntervals.add(interval1);
				
			}else{
				newDimensions.add(dimension1);
				newIntervals.add(interval1);
				
				newDimensions.add(dimension2);
				newIntervals.add(interval2);
				
			}
			
		}
	
		Context newUnit = new Context(newDimensions, newIntervals);
		
		if(ContextPruning.densityPruning(newUnit, tau)){
			return null;
		}
		return newUnit;
	}
	
	public String print(DatumEncoder encoder){
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < dimensions.size(); i++){
			sb.append( intervals.get(i).print(encoder) + " ");
		}
		return sb.toString();
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < dimensions.size(); i++){
			sb.append(dimensions.get(i) + ":" + intervals.get(i).toString() + " ");
		}
		return sb.toString();
	}

	
	public void clear(){
		dimensions.clear();
		intervals.clear();
		
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
}

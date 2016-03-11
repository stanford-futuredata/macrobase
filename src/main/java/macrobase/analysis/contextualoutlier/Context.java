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
	private List<Integer> dimensions = new ArrayList<Integer>();
	private List<Interval> intervals = new ArrayList<Interval>();;
	
	private int size = -1;
	private List<Context> parents = new ArrayList<Context>();
	
	//the sample is maintained in the lattice
	private HashSet<Datum> sample = new HashSet<Datum>();
	
	/**
	 * Global context
	 */
	public Context(HashSet<Datum> sample){
		this.sample = sample;
	}
	
	/**
	 * Initialize a one dimensional context
	 * @param dimension
	 * @param interval
	 * @param parent
	 */
	public Context(int dimension, Interval interval, Context parent){
		dimensions.add(dimension);
		intervals.add(interval);
		parents.add(parent);
		
		for(Datum d: parent.sample){
			if(containDatum(d)){
				sample.add(d);
			}
		}
	}
	
	public Context(List<Integer> dimensions, List<Interval> intervals, Context parent1, Context parent2) {
		this.dimensions = dimensions;
		this.intervals = intervals;
		
		parents.add(parent1);
		parents.add(parent2);
	
		sample.addAll(parent1.sample);
		sample.retainAll(parent2.sample);
		
	}
	

	public List<Integer> getDimensions(){
		return dimensions;
	}
	
	/**
	 * Get all the contextual data, given the whole dataset
	 * @param data
	 * @return
	 */
	public List<Datum> getContextualData(List<Datum> data){
		List<Datum> contextualData = new ArrayList<Datum>();
    	
		
		//set sample 
		/*
		Random rnd = new Random();
		int numSample = 1000;
    	List<Datum> sampleContextualData = new ArrayList<Datum>();
    	int i = 0;
    	*/
    	
    	for(Datum d: data){    		
    		if(containDatum(d) ){
    			contextualData.add(d);
    			/*
    			if(sampleContextualData.size() < numSample){
    				sampleContextualData.add(d);
    			}else{
    				int j = rnd.nextInt(i); //j in [0,i)
    				
    				if(j < sampleContextualData.size()){
    					sampleContextualData.set(j, d);
    				}
    			}
    			i++;
    			*/
    		}
    			
    	}
    	
    	//sample.addAll(sampleContextualData);
    	setSize(contextualData.size());
    	return contextualData;
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
	
		Context newUnit = new Context(newDimensions, newIntervals, this, other);
		
		
		if(ContextPruning.densityPruning(newUnit, tau)){
			return null;
		}
		
		if(ContextPruning.dependencyPruning(newUnit)){
			return null;
		}
		
		return newUnit;
	}
	
	public String print(DatumEncoder encoder){
		if(dimensions.size() == 0){
			return "Global Context: ";
		}
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < dimensions.size(); i++){
			sb.append( intervals.get(i).print(encoder) + " ");
		}
		return sb.toString();
	}
	
	@Override
	public String toString(){
		if(dimensions.size() == 0){
			return "Global Context: ";
		}
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
	
	
	public HashSet<Datum> getSample(){
		return sample;
	}
	
	public List<Context> getParents(){
		return parents;
	}
}

package macrobase.analysis.contextualoutlier;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

public class ContextualOutlierDetector{
    private static final Logger log = LoggerFactory.getLogger(ContextualOutlierDetector.class);

    
    private OutlierDetector detector; //the detector used for every context
    
    
    private List<String> contextualDiscreteAttributes;
    private List<String> contextualDoubleAttributes;
    private int totalContextualDimensions;
    
    
    Context globalContext = new Context();
    
    
    private double denseContextTau;
    private int numIntervals;
    
    //This is the outliers detected for every dense context
    //could've stored Context,OutlierDetector.BatchResult, but waste a lot of memory
    private Map<Context,List<Datum>> context2Outliers = new HashMap<Context,List<Datum>>();
    
    public ContextualOutlierDetector(OutlierDetector outlierDetector,
    		List<String> contextualDiscreteAttributes,
    		List<String> contextualDoubleAttributes,
    		double denseContextTau,
    		int numIntervals){
    	
    	this.detector = outlierDetector;
    	
    	this.contextualDiscreteAttributes = contextualDiscreteAttributes;
    	this.contextualDoubleAttributes = contextualDoubleAttributes;
    	this.denseContextTau = denseContextTau;
    	this.numIntervals = numIntervals;
    	
    	totalContextualDimensions = contextualDiscreteAttributes.size() + contextualDoubleAttributes.size();
    }
    
    public Map<Context,List<Datum>> getContextualOutliers(){
    	return context2Outliers;
    }
    
    public void searchContextualOutliers(List<Datum> data, double zScore){
    	
    	Stopwatch sw = Stopwatch.createUnstarted();
    	
    	
    	log.debug("Find global context outliers on data size: " + data.size());
    	sw.start();
    	
        
        contextualOutlierDetection(data,globalContext,zScore);
    	
    	List<Datum> remainingData = null;
    	//should not exclude outlier data points from bigger contexts
    	remainingData = data;
    	
    	
    	sw.stop();
    	long globalOutlierDetecionTime = sw.elapsed(TimeUnit.MILLISECONDS);
    	sw.reset();
    	log.debug("Done global context outlier remaining data size {} : (duration: {}ms)", remainingData.size(),globalOutlierDetecionTime);
    	
    	
    	initContextPruning(data);
    	
    	List<LatticeNode> preLatticeNodes = new ArrayList<LatticeNode>();
    	List<LatticeNode> curLatticeNodes = new ArrayList<LatticeNode>();
    	for(int level = 1; level <= totalContextualDimensions; level++){
			
    		
    		log.debug("Build {}-dimensional contexts on all attributes",level);
    		sw.start();
    		if(level == 1){
    			curLatticeNodes = buildOneDimensionalLatticeNodes(remainingData);
        	}else{
        		curLatticeNodes = levelUpLattice(preLatticeNodes, remainingData);	
        	}
    		sw.stop();
    		long latticeNodesBuildTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
    		sw.reset();
        	log.debug("Done building {}-dimensional contexts on all attributes (duration: {}ms)", level,latticeNodesBuildTimeCurLevel);
        	
        	//free up memory of preLatticeNodes
        	for(LatticeNode node: preLatticeNodes){
        		for(Context context: node.getDenseContexts()){
        			if(!context2Outliers.containsKey(context))
        				context.clear();
        		}
        		node.clear();
        	}
        	preLatticeNodes.clear();	
        	
        	
        	log.debug("Memory Usage: {}", checkMemoryUsage());
    		
    		if(curLatticeNodes.size() == 0){
    			log.debug("No more dense contexts, thus no need to level up anymore");
    			break;
    		}
    			
    		
        	log.debug("Find {}-dimensional contextual outliers",level);
        	sw.start();
        	int numDenseContextsCurLevel = 0;
        	//run contextual outlier detection
        	for(LatticeNode node: curLatticeNodes){
        		for(Context context: node.getDenseContexts()){
        			contextualOutlierDetection(remainingData,context,zScore);
        			numDenseContextsCurLevel++;
        		}
        	}
        	sw.stop();
        	long contextualOutlierDetectionTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
        	sw.reset();
        	log.debug("Done Find {}-dimensional contextual outliers (duration: {}ms)", level, contextualOutlierDetectionTimeCurLevel);
        	log.debug("Done Find {}-dimensional contextual outliers, there are {} dense contexts(average duration per context: {}ms)", level, numDenseContextsCurLevel,(numDenseContextsCurLevel == 0)?0:contextualOutlierDetectionTimeCurLevel/numDenseContextsCurLevel);
        	log.debug("Done Find {}-dimensional contextual outliers, Context Pruning: {}", level,ContextPruning.print());
        	log.debug("Done Find {}-dimensional contextual outliers, so far, Total Contextual OutlierDetection Runs: {}", level,totalContextualOutlierDetectionRuns);
            log.debug("----------------------------------------------------------");

        	
			preLatticeNodes = curLatticeNodes;
		}
    	
    	
    }
    
    
    private void initContextPruning(List<Datum> data){
    	
    	ContextPruning.detector = detector;
    	ContextPruning.data = data;
    	//Init a sample of the entire dataset
    	List<Datum> sampleData = new ArrayList<Datum>();
    	ContextPruning.alpha = 0.05;
    	//numSamples is determined by confidence and error margin
    	//95% confidence, +-B error bound
    	//B = 1% => 10000
    	int numSample = 10000;
    	
    	Random rnd = new Random();
		for(int i = 0; i < data.size(); i++){
			Datum d = data.get(i);
			if(sampleData.size() < numSample){
				sampleData.add(d);
			}else{
				int j = rnd.nextInt(i); //j in [0,i)
				if(j < sampleData.size()){
					sampleData.set(j, d);
				}
			}
			
		}
		
		ContextPruning.sampleData = sampleData;
    }
    private String checkMemoryUsage(){
    	Runtime runtime = Runtime.getRuntime();

    	NumberFormat format = NumberFormat.getInstance();

    	StringBuilder sb = new StringBuilder();
    	long maxMemory = runtime.maxMemory();
    	long allocatedMemory = runtime.totalMemory();
    	long freeMemory = runtime.freeMemory();

    	sb.append("free memory: " + format.format(freeMemory / 1024) + "<br/>");
    	sb.append("allocated memory: " + format.format(allocatedMemory / 1024) + "<br/>");
    	sb.append("max memory: " + format.format(maxMemory / 1024) + "<br/>");
    	sb.append("total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024) + "<br/>");
    	return sb.toString();
    }
    
    
  /**
   * Walking up the lattice, construct the lattice node, when include those lattice nodes that contain at least one dense context
   * @param latticeNodes
   * @param data
   * @return
   */
	private List<LatticeNode> levelUpLattice(List<LatticeNode> latticeNodes, List<Datum> data){
		
		//sort the subspaces by their dimensions
		Stopwatch sw = Stopwatch.createUnstarted();
		
		log.debug("\tSorting lattice nodes in level {} by their dimensions " , latticeNodes.get(0).dimensions.size());
		sw.start();
		
		
		List<LatticeNode> latticeNodeByDimensions = new ArrayList<LatticeNode>(latticeNodes);
	    Collections.sort(latticeNodeByDimensions, new LatticeNode.DimensionComparator());

	    sw.stop();
	    long sortingTime = sw.elapsed(TimeUnit.MILLISECONDS);
	    sw.reset();
		log.debug("\tDone Sorting lattice nodes in level {} by their dimensions (duration: {}ms)" , latticeNodes.get(0).dimensions.size(), sortingTime);

	    
	    //find out dense candidate subspaces 
	    List<LatticeNode> result = new ArrayList<LatticeNode>();
		
	    
		log.debug("\tJoining lattice nodes in level {} by their dimensions " , latticeNodes.get(0).dimensions.size());
		sw.start();
		
		int numLatticeNodeJoins = 0;
		int numDenseContexts = 0;
		for(int i = 0; i < latticeNodeByDimensions.size(); i++ ){
			for(int j = i +1; j < latticeNodeByDimensions.size(); j++){
	    		
	    		LatticeNode s1 = latticeNodeByDimensions.get(i);
	    		LatticeNode s2 = latticeNodeByDimensions.get(j);
	    		LatticeNode joined = s1.join(s2, data, denseContextTau);
	    		
	    		if(joined != null){
	    			numLatticeNodeJoins++;
	    			//only interested in nodes that have dense contexts
	    			if(joined.getDenseContexts().size() != 0){
	    				result.add(joined);
	    				numDenseContexts += joined.getDenseContexts().size();
	    			}
	    				
	    		}
	    	}
	    }
	    
	    sw.stop();
	    long joiningTime = sw.elapsed(TimeUnit.MILLISECONDS);
	    sw.reset();
	    
		log.debug("\tDone Joining lattice nodes in level {} by their dimensions (duration: {}ms)" , latticeNodes.get(0).dimensions.size(), joiningTime);
		log.debug("\tDone Joining lattice nodes in level {} by their dimensions,"
				+ " there are {} joins and {} dense contexts (average duration per lattice node pair join: {}ms)" , 
				latticeNodes.get(0).dimensions.size(), numLatticeNodeJoins,numDenseContexts,  (numLatticeNodeJoins==0)?0:joiningTime/numLatticeNodeJoins);

		
		
		return result;
	}
	
	
    
	private int totalContextualOutlierDetectionRuns = 0;
    /**
     * Run outlier detection algorithm on contextual data
     * The algorithm has to use zScore in contextual outlier detection
     * @param data
     * @param tids
     * @param zScore
     * @return
     */
    public void contextualOutlierDetection(List<Datum> data, Context context, double zScore){
    	
    	if(context.getParents().size() == 2 &&
    			ContextPruning.sameDistribution(context.getParents().get(0), context.getParents().get(1))){
    		return;
    	}
    	
    	List<Datum> contextualData = context.getContextualData(data);
    	
    	
    	//Just did density estimation before
    	double realDensity = (double)contextualData.size() / data.size();
    	if(realDensity < denseContextTau){
    		return;
    	}
    	
    	//pdf pruning
    	if(context.getParents().size() > 0){
    		if(ContextPruning.sameDistribution(context, context.getParents().get(0))){
    			return;
    		}else if(context.getParents().size() > 1 && 
    				ContextPruning.sameDistribution(context, context.getParents().get(1))){
    			return;
    		}
    	}
    	
        OutlierDetector.BatchResult or = detector.classifyBatchByZScoreEquivalent(contextualData, zScore);
       
        totalContextualOutlierDetectionRuns++;
        List<Datum> outliers = new ArrayList<Datum>();
        if(or.getOutliers().size() != 0){
        	for(DatumWithScore o: or.getOutliers()){
        		outliers.add(o.getDatum());
        	}
        	 context2Outliers.put(context, outliers);
        }
        
    }
    
    /**
	 * Find one dimensional lattice nodes with dense contexts
	 * @param data
	 * @return
	 */
	private List<LatticeNode> buildOneDimensionalLatticeNodes(List<Datum> data){
		
		
		//create subspaces
		List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
				
		for(int dimension = 0; dimension < totalContextualDimensions; dimension++){
			LatticeNode ss = new LatticeNode(dimension);
			List<Context> denseContexts = initOneDimensionalDenseContexts(data, dimension);
			for(Context denseContext: denseContexts){
				ss.addDenseContext(denseContext);
				log.debug(denseContext.toString());
			}
			latticeNodes.add(ss);
		}
		
		return latticeNodes;
	}
	
	
	/**
	 * Initialize one dimensional dense contexts
	 * The number of passes of data is O(totalContextualDimensions)
	 * @param data
	 * @param dimension
	 * @return
	 */
	private List<Context> initOneDimensionalDenseContexts(List<Datum> data, int dimension){
		int discreteDimensions = contextualDiscreteAttributes.size();
		
		
		List<Context> result = new ArrayList<Context>();
		
		if(dimension < discreteDimensions){
			Map<Integer,Integer> distinctValue2Count = new HashMap<Integer,Integer>();
			for(Datum datum: data){
				Integer value = datum.getContextualDiscreteAttributes().get(dimension);
				if(distinctValue2Count.containsKey(value)){
					distinctValue2Count.put(value, distinctValue2Count.get(value) + 1);
				}else{
					distinctValue2Count.put(value, 1);
				}
				
			}
			for(Integer value: distinctValue2Count.keySet()){
				//boolean denseContext = !ContextPruning.densityPruning(data.size(), distinctValue2Count.get(value), denseContextTau);
				boolean denseContext = ( (double) distinctValue2Count.get(value) / data.size() >= denseContextTau)?true:false;
				if(denseContext){
					Interval interval = new IntervalDiscrete(dimension,contextualDiscreteAttributes.get(dimension),value);
					Context context = new Context(dimension, interval, globalContext);
					result.add(context);
				}
			}
		}else{
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			//find out the min, max
			for(Datum datum: data){
				double value = datum.getContextualDoubleAttributes().getEntry(dimension - discreteDimensions );
				if(value > max){
					max = value;
				}
				if(value < min){
					min = value;
				}
			}
			HashSet<Interval> allIntervals = new HashSet<Interval>();
			// divide the interval into numIntervals
			double step = (max - min) / numIntervals;
			double start = min;
			for(int i = 0; i < numIntervals; i++){
				if(i != numIntervals - 1){
					Interval interval = new IntervalDouble(dimension,contextualDoubleAttributes.get(dimension - discreteDimensions), start, start + step);
					start += step;
					allIntervals.add(interval);
				}else{
					//make the max a little bit larger
					Interval interval = new IntervalDouble(dimension, contextualDoubleAttributes.get(dimension - discreteDimensions),start, max + 0.000001);
					allIntervals.add(interval);
				}
			}
			//count the interval
			HashMap<Interval,Integer> interval2Count = new HashMap<Interval,Integer>();
			for(Datum datum: data){
				double value = datum.getContextualDoubleAttributes().getEntry(dimension - discreteDimensions );
				for(Interval interval: allIntervals){
					if(interval.contains(value)){
						if(interval2Count.containsKey(interval)){
							interval2Count.put(interval, interval2Count.get(interval)+1);
						}else{
							interval2Count.put(interval,1);
						}
						break;
					}
				}
			}
			for(Interval interval: interval2Count.keySet()){
				//boolean denseContext =!ContextPruning.densityPruning(data.size(), interval2Count.get(interval), denseContextTau);
				boolean denseContext = ( (double) interval2Count.get(interval) / data.size() >= denseContextTau)?true:false;
				if(denseContext){
					Context context = new Context(dimension, interval,globalContext);
					result.add(context);
				}
			}
		}
		
		return result;
	}
    
}

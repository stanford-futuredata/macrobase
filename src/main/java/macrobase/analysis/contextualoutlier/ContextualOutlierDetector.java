package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

public class ContextualOutlierDetector{
    private static final Logger log = LoggerFactory.getLogger(ContextualOutlierDetector.class);

    
    private BatchTrainScore detector; //the detector used for every context
    
    
    private List<String> contextualDiscreteAttributes;
    private List<String> contextualDoubleAttributes;
    private int totalContextualDimensions;
    
    
    private double denseContextTau;
    private int numIntervals;
    
    //This is the outliers detected for every dense context
    private Map<Context,BatchTrainScore.BatchResult> context2Outliers = new HashMap<Context,BatchTrainScore.BatchResult>();
    
    public ContextualOutlierDetector(BatchTrainScore outlierDetector,
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
    
    public Map<Context,BatchTrainScore.BatchResult> getContextualOutliers(){
    	return context2Outliers;
    }
    
    public void searchContextualOutliers(List<Datum> data, double zScore){
    	
    	log.debug("Find global context outliers on data size: " + data.size());
    	BatchTrainScore.BatchResult or = detector.classifyBatchByZScoreEquivalent(data, zScore);
    	List<Datum> remainingData = new ArrayList<Datum>();
    	for(DatumWithScore ds: or.getInliers()){
    		remainingData.add(ds.getDatum());
    	}
    	log.debug("Done global context outlier remaining data size: " + remainingData.size());
    	
    	
    	List<LatticeNode> preLatticeNodes = null;
    	List<LatticeNode> curLatticeNodes = null;
    	for(int level = 1; level <= totalContextualDimensions; level++){
			
    		if(level == 1){
        		log.debug("Build one-dimensional contexts on all attributes");
        		curLatticeNodes = buildOneDimensionalLatticeNodes(remainingData);
        	}else{
        		log.debug("Build {}-dimensional contexts on all attributes",level);
        		curLatticeNodes = levelUpLattice(preLatticeNodes, remainingData);
    			
        	}
        	log.debug("Find {}-dimensional contextual outliers",level);
        	//run contextual outlier detection
        	for(LatticeNode node: curLatticeNodes){
        		for(Context context: node.getDenseContexts()){
        			contextualOutlierDetection(remainingData,context,zScore);
        		}
        	}
        	
			preLatticeNodes = curLatticeNodes;
		}
    	
    }
    
  /**
   * Walking up the lattice, construct the lattice node, when include those lattice nodes that contain at least one dense context
   * @param latticeNodes
   * @param data
   * @return
   */
	private List<LatticeNode> levelUpLattice(List<LatticeNode> latticeNodes, List<Datum> data){
		
		//sort the subspaces by their dimensions
		List<LatticeNode> latticeNodeByDimensions = new ArrayList<LatticeNode>(latticeNodes);
	    Collections.sort(latticeNodeByDimensions, new LatticeNode.DimensionComparator());

	    //find out dense candidate subspaces 
	    List<LatticeNode> result = new ArrayList<LatticeNode>();
		
	    for(int i = 0; i < latticeNodeByDimensions.size(); i++ ){
	    	for(int j = i +1; j < latticeNodeByDimensions.size(); j++){
	    		LatticeNode s1 = latticeNodeByDimensions.get(i);
	    		LatticeNode s2 = latticeNodeByDimensions.get(j);
	    		LatticeNode joined = s1.join(s2, data.size(), denseContextTau);
	    		
	    		if(joined != null){
	    			result.add(joined);
	    		}
	    	}
	    }
	    
	    
		return result;
	}
	
	
    
    /**
     * Run outlier detection algorithm on contextual data
     * The algorithm has to use zScore in contextual outlier detection
     * @param data
     * @param tids
     * @param zScore
     * @return
     */
    private BatchTrainScore.BatchResult contextualOutlierDetection(List<Datum> data, Context context, double zScore){
    	
    	List<Datum> contextualData = new ArrayList<Datum>();
    	
    	for(int tid: context.getTIDs()){
    		contextualData.add(data.get(tid));
    	}
    	
        BatchTrainScore.BatchResult or = detector.classifyBatchByZScoreEquivalent(contextualData, zScore);
       
        if(or.getOutliers().size() != 0){
        	context2Outliers.put(context, or);
        }
        
        //excluding outlier tids from the context
        for(DatumWithScore datumWithScore: or.getOutliers()){
        	Datum outlierDatum = datumWithScore.getDatum();
        	int outlierTID = data.indexOf(outlierDatum);
        	
        	context.removeTID(outlierTID);
        }
        return or;
    }
    
    /**
	 * Find one dimensional lattice nodes with dense contexts
	 * @param data
	 * @return
	 */
	private List<LatticeNode> buildOneDimensionalLatticeNodes(List<Datum> data){
		
		List<Context> oneDimensionalUnits = initOneDimensionalContexts(data);
		
		//find out the dense one dimensional contexts
		Map<Integer,List<Context>> dimension2OneDimensionalDenseUnits = new HashMap<Integer, List<Context>>();
		for(Context context: oneDimensionalUnits){
			if(context.isDense(data.size(), denseContextTau)){
				
				int dimension = context.getDimensions().get(0);
				if(!dimension2OneDimensionalDenseUnits.containsKey(dimension))
					dimension2OneDimensionalDenseUnits.put(dimension, new ArrayList<Context>());
				
				dimension2OneDimensionalDenseUnits.get(dimension).add(context);
			}
		}
		
		//create subspaces
		List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
		for(int dimension: dimension2OneDimensionalDenseUnits.keySet()){
			LatticeNode ss = new LatticeNode(dimension);
			for(Context denseUnit: dimension2OneDimensionalDenseUnits.get(dimension)){
				ss.addDenseContext(denseUnit);
			}
			latticeNodes.add(ss);
		}
		
		
		return latticeNodes;
	}
	
	
    /**
	 * Initialize one dimensional contexts
	 * @param data
	 * @return
	 */
	private List<Context> initOneDimensionalContexts(List<Datum> data){
		List<Context> result = new ArrayList<Context>();
		for(int dimension = 0; dimension < totalContextualDimensions; dimension++){
			result.addAll( initOneDimensionalContexts(data,dimension) );
		}
		return result;
	}
	
	
	private List<Context> initOneDimensionalContexts(List<Datum> data, int dimension){
		int discreteDimensions = contextualDiscreteAttributes.size();
		
		
		List<Context> result = new ArrayList<Context>();
		
		if(dimension < discreteDimensions){
			HashSet<Integer> distinctValues = new HashSet<Integer>();
			for(Datum datum: data){
				distinctValues.add(datum.getContextualDiscreteAttributes().get(dimension));
			}
			for(Integer value: distinctValues){
				Interval interval = new IntervalDiscrete(dimension,contextualDiscreteAttributes.get(dimension),value);
				Context context = new Context(dimension, interval);
				result.add(context);
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
			
			// divide the interval into numIntervals
			double step = (max - min) / numIntervals;
			double start = min;
			for(int i = 0; i < numIntervals; i++){
				if(i != numIntervals - 1){
					Interval interval = new IntervalDouble(dimension,contextualDoubleAttributes.get(dimension - discreteDimensions), start, start + step);
					start += step;
					Context context = new Context(dimension, interval);
					result.add(context);
				}else{
					//make the max a little bit larger
					Interval interval = new IntervalDouble(dimension, contextualDoubleAttributes.get(dimension - discreteDimensions),start, max + 0.000001);
					Context context = new Context(dimension, interval);
					result.add(context);
				}
				
			}
		}
		
		
		//add the support tids of contexts
		for(int t = 0; t < data.size(); t++){
			Datum datum = data.get(t);
			for(Context context: result){
				if(context.containDatum(datum)){
					context.addTID(t);
				}
			}
		}
				
				
		return result;
	}
    
}

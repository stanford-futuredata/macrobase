package macrobase.analysis.contextualoutlier;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import macrobase.analysis.outlier.BinnedKDE;
import macrobase.analysis.outlier.KDE;
import macrobase.analysis.outlier.MAD;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.MovingAverage;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.TreeKDE;
import macrobase.analysis.outlier.ZScore;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;

public class ContextualOutlierDetector{
    private static final Logger log = LoggerFactory.getLogger(ContextualOutlierDetector.class);

    
    
    private MacroBaseConf conf;
    private List<String> contextualDiscreteAttributes;
    private List<String> contextualDoubleAttributes;
    private int totalContextualDimensions;
    
    
    Context globalContext;
    
    
    private double denseContextTau;
    private int numIntervals;
    private int maxPredicates;
    //This is the outliers detected for every dense context
    //could've stored Context,OutlierDetector.BatchResult, but waste a lot of memory
    private Map<Context,List<Datum>> context2Outliers = new HashMap<Context,List<Datum>>();
    
    public ContextualOutlierDetector(MacroBaseConf conf,
    		List<String> contextualDiscreteAttributes,
    		List<String> contextualDoubleAttributes,
    		double denseContextTau,
    		int numIntervals){
    	
    	this.conf = conf;
    	this.contextualDiscreteAttributes = contextualDiscreteAttributes;
    	this.contextualDoubleAttributes = contextualDoubleAttributes;
    	this.denseContextTau = denseContextTau;
    	this.numIntervals = numIntervals;
    	
    	this.maxPredicates = conf.getInt(MacroBaseConf.CONTEXTUAL_MAX_PREDICATES,MacroBaseDefaults.CONTEXTUAL_MAX_PREDICATES);
    	
    	this.totalContextualDimensions = contextualDiscreteAttributes.size() + contextualDoubleAttributes.size();
    	log.debug("There are {} contextualDiscreteAttributes, and {} contextualDoubleAttributes" , contextualDiscreteAttributes.size(),contextualDoubleAttributes.size());
    }
    
    public Map<Context,List<Datum>> getContextualOutliers(){
    	return context2Outliers;
    }
    
    
    
    /**
     * Interface 1: search all contextual outliers
     * @param data
     * @param zScore
     * @param encoder
     */
    public void searchContextualOutliers(List<Datum> data, double zScore, DatumEncoder encoder){
    	
    	Stopwatch sw = Stopwatch.createUnstarted();
    	
    	
    	log.debug("Find global context outliers on data num tuples: {} , MBs {} ",data.size());
    	sw.start();
    	
    	HashSet<Datum> sample = randomSampling(data);
        globalContext = new Context(sample);
    	ContextPruning.data = data;
    	ContextPruning.sample = sample;
    	ContextPruning.alpha = 0.05;
    	
    	
        contextualOutlierDetection(data,globalContext,zScore, encoder);
    	
    	sw.stop();
    	long globalOutlierDetecionTime = sw.elapsed(TimeUnit.MILLISECONDS);
    	sw.reset();
    	log.debug("Done global context outlier remaining data size {} : (duration: {}ms)", data.size(),globalOutlierDetecionTime);
    	
    	
    	
    	List<LatticeNode> preLatticeNodes = new ArrayList<LatticeNode>();
    	List<LatticeNode> curLatticeNodes = new ArrayList<LatticeNode>();
    	for(int level = 1; level <= totalContextualDimensions; level++){
			
    		if(level > maxPredicates)
    			break;
    		
    		log.debug("Build {}-dimensional contexts on all attributes",level);
    		sw.start();
    		if(level == 1){
    			curLatticeNodes = buildOneDimensionalLatticeNodes(data);
        	}else{
        		curLatticeNodes = levelUpLattice(preLatticeNodes, data);	
        	}
    		sw.stop();
    		long latticeNodesBuildTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
    		sw.reset();
        	log.debug("Done building {}-dimensional contexts on all attributes (duration: {}ms)", level,latticeNodesBuildTimeCurLevel);
        	
        	
        	
        	
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
        			contextualOutlierDetection(data,context,zScore,encoder);
        			numDenseContextsCurLevel++;
        		}
        	}
        	sw.stop();
        	long contextualOutlierDetectionTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
        	sw.reset();
        	log.debug("Done Find {}-dimensional contextual outliers (duration: {}ms)", level, contextualOutlierDetectionTimeCurLevel);
        	log.debug("Done Find {}-dimensional contextual outliers, there are {} dense contexts(average duration per context: {}ms)", level, numDenseContextsCurLevel,(numDenseContextsCurLevel == 0)?0:contextualOutlierDetectionTimeCurLevel/numDenseContextsCurLevel);
        	log.debug("Done Find {}-dimensional contextual outliers, Context Pruning: {}", level,ContextPruning.print());
            log.debug("Done Find {}-dimensional contextual outliers, densityPruning2: {}, "
            		+ "numOutlierDetectionRunsWithoutTrainingWithoutScoring: {},  "
            		+ "numOutlierDetectionRunsWithoutTrainingWithScoring: {},  "
            		+ "numOutlierDetectionRunsWithTrainingWithScoring: {}", 
            		level,densityPruning2,
            		numOutlierDetectionRunsWithoutTrainingWithoutScoring,
            		numOutlierDetectionRunsWithoutTrainingWithScoring,
            		numOutlierDetectionRunsWithTrainingWithScoring);
            log.debug("----------------------------------------------------------");

        	
            //free up memory 
        	if(level >= 2){
        		for(LatticeNode node: preLatticeNodes){
        			for(Context context: node.getDenseContexts()){
        				context2BitSet.remove(context);
        			}
        		}
        	}
        	
        	
			preLatticeNodes = curLatticeNodes;
			
			
		}
    	
    	
    }
    
    /**
     * Interface 2: Given some outliers, search contexts for which they are outliers
     * @param data
     * @param zScore
     * @param encoder
     * @param inputOutliers
     */
    public List<Context> searchContextGivenOutliers(List<Datum> data, double zScore, DatumEncoder encoder, List<Datum> inputOutliers){
    	
    	//result contexts that have the input outliers
    	List<Context> result = new ArrayList<Context>();
    	
    	Stopwatch sw = Stopwatch.createUnstarted();
    	
    	log.debug("Find global context outliers on data num tuples: {} , MBs {} ",data.size());
    	sw.start();
    	
    	HashSet<Datum> sample = randomSampling(data);
        globalContext = new Context(sample);
    	ContextPruning.data = data;
    	ContextPruning.sample = sample;
    	ContextPruning.alpha = 0.05;
    	
    	
        List<Datum> globalOutliers = contextualOutlierDetection(data,globalContext,zScore, encoder);
    	if(globalOutliers != null && globalOutliers.contains(inputOutliers)){
    		result.add(globalContext);
    	}
        
        
    	sw.stop();
    	long globalOutlierDetecionTime = sw.elapsed(TimeUnit.MILLISECONDS);
    	sw.reset();
    	log.debug("Done global context outlier remaining data size {} : (duration: {}ms)", data.size(),globalOutlierDetecionTime);
    	
    	
    	
    	List<LatticeNode> preLatticeNodes = new ArrayList<LatticeNode>();
    	List<LatticeNode> curLatticeNodes = new ArrayList<LatticeNode>();
    	for(int level = 1; level <= totalContextualDimensions; level++){
			
    		if(level > maxPredicates)
    			break;
    		
    		log.debug("Build {}-dimensional contexts on all attributes",level);
    		sw.start();
    		if(level == 1){
    			curLatticeNodes = buildOneDimensionalLatticeNodesGivenOutliers(data, inputOutliers);
        	}else{
        		curLatticeNodes = levelUpLattice(preLatticeNodes, data);	
        	}
    		sw.stop();
    		long latticeNodesBuildTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
    		sw.reset();
        	log.debug("Done building {}-dimensional contexts on all attributes (duration: {}ms)", level,latticeNodesBuildTimeCurLevel);
        	
        	
        	
        	
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
        			List<Datum> outliers = contextualOutlierDetection(data,context,zScore,encoder);
        			if(outliers != null && outliers.containsAll(inputOutliers)){
        	    		result.add(context);
        	    	}
        			numDenseContextsCurLevel++;
        		}
        	}
        	sw.stop();
        	long contextualOutlierDetectionTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
        	sw.reset();
        	log.debug("Done Find {}-dimensional contextual outliers (duration: {}ms)", level, contextualOutlierDetectionTimeCurLevel);
        	log.debug("Done Find {}-dimensional contextual outliers, there are {} dense contexts(average duration per context: {}ms)", level, numDenseContextsCurLevel,(numDenseContextsCurLevel == 0)?0:contextualOutlierDetectionTimeCurLevel/numDenseContextsCurLevel);
        	log.debug("Done Find {}-dimensional contextual outliers, Context Pruning: {}", level,ContextPruning.print());
        	log.debug("Done Find {}-dimensional contextual outliers, densityPruning2: {}, "
            		+ "numOutlierDetectionRunsWithoutTrainingWithoutScoring: {},  "
            		+ "numOutlierDetectionRunsWithoutTrainingWithScoring: {},  "
            		+ "numOutlierDetectionRunsWithTrainingWithScoring: {}", 
            		level,densityPruning2,
            		numOutlierDetectionRunsWithoutTrainingWithoutScoring,
            		numOutlierDetectionRunsWithoutTrainingWithScoring,
            		numOutlierDetectionRunsWithTrainingWithScoring);
            log.debug("----------------------------------------------------------");       
            
        	
            //free up memory 
        	if(level >= 2){
        		for(LatticeNode node: preLatticeNodes){
        			for(Context context: node.getDenseContexts()){
        				context2BitSet.remove(context);
        			}
        		}
        	}
        	
        	
			preLatticeNodes = curLatticeNodes;
			
			
		}
    	return result;
    }
    
    
    
    private HashSet<Datum> randomSampling(List<Datum> data){
    	
    	List<Datum> sampleData = new ArrayList<Datum>();
    	
    	int minSampleSize = 100;
    	int numSample = (int) (minSampleSize / denseContextTau);
    	
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
		
		return new HashSet<Datum>(sampleData);
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
	
	
    private int densityPruning2 = 0;
    private int numOutlierDetectionRunsWithoutTrainingWithoutScoring = 0;
    private int numOutlierDetectionRunsWithoutTrainingWithScoring = 0;
    private int numOutlierDetectionRunsWithTrainingWithScoring = 0;
    /**
     * Run outlier detection algorithm on contextual data
     * The algorithm has to use zScore in contextual outlier detection
     * @param data
     * @param tids
     * @param zScore
     * @return
     */
    public List<Datum> contextualOutlierDetection(List<Datum> data, Context context, double zScore, DatumEncoder encoder){
    	
    
    	
    	OutlierDetector.BatchResult or = null;
    	//HashSet<Datum> contextualData = context.getContextualData(data,context2Data);
    	//context2Data.put(context, contextualData);
    	BitSet bs = context.getContextualBitSet(data,context2BitSet);
    	context2BitSet.put(context, bs);
    	
    	Context p1 = (context.getParents().size() > 0)?context.getParents().get(0):null;
    	Context p2 = (context.getParents().size() > 1)?context.getParents().get(1):null;
    	
    	if(p1 != null && ContextPruning.sameDistribution(context, p1)){
    		//training
    		context.setDetector(p1.getDetector());

    		//scoring
    		if(!context2Outliers.containsKey(p1)){
    			//parent does not contain outliers
    			numOutlierDetectionRunsWithoutTrainingWithoutScoring++;
    		}else{
    			List<Integer> indexes = bitSet2Indexes(bs);
            	List<Datum> contextualData = new ArrayList<Datum>();
            	for(Integer index: indexes){
            		contextualData.add(data.get(index));
            	}
            	context.setSize(contextualData.size());
            			
            	//Just did density estimation before
            	double realDensity = (double)contextualData.size() / data.size();
            	if(realDensity < denseContextTau){
            		densityPruning2++;
            		return null;
            	}
            	or = context.getDetector().classifyBatchByZScoreEquivalentWithoutTraining(new ArrayList<Datum>(contextualData), zScore);
        		numOutlierDetectionRunsWithoutTrainingWithScoring++;
    		}
        	
    	}else if(p2 != null && ContextPruning.sameDistribution(context, p2)){
    		
    		//training
    		context.setDetector(p2.getDetector());
    		
    		//scoring
    		if(!context2Outliers.containsKey(p2)){
    			//parent does not contain outliers
    			numOutlierDetectionRunsWithoutTrainingWithoutScoring++;
    		}else{
    			List<Integer> indexes = bitSet2Indexes(bs);
            	List<Datum> contextualData = new ArrayList<Datum>();
            	for(Integer index: indexes){
            		contextualData.add(data.get(index));
            	}
            	context.setSize(contextualData.size());
            			
            	//Just did density estimation before
            	double realDensity = (double)contextualData.size() / data.size();
            	if(realDensity < denseContextTau){
            		densityPruning2++;
            		return null;
            	}
        		or = context.getDetector().classifyBatchByZScoreEquivalentWithoutTraining(new ArrayList<Datum>(contextualData), zScore);
        		numOutlierDetectionRunsWithoutTrainingWithScoring++;
    		}
        	
    	}else{
    		
    		//training
    		try {
				context.setDetector(constructDetector());
			} catch (ConfigurationException e) {
				e.printStackTrace();
			}
    		
    		//scoring
    		List<Integer> indexes = bitSet2Indexes(bs);
        	List<Datum> contextualData = new ArrayList<Datum>();
        	for(Integer index: indexes){
        		contextualData.add(data.get(index));
        	}
        	context.setSize(contextualData.size());
        	
        	
    		or = context.getDetector().classifyBatchByZScoreEquivalent(new ArrayList<Datum>(contextualData), zScore);
    		numOutlierDetectionRunsWithTrainingWithScoring++;
    	}
    	
    	
    	
        List<Datum> outliers = new ArrayList<Datum>();
        if(or != null && or.getOutliers().size() != 0){
        	//outlier explanation
        	//explainContextualOutliers(context,or,encoder);
        	
        	
        	for(DatumWithScore o: or.getOutliers()){
        		outliers.add(o.getDatum());
        	}
        	 context2Outliers.put(context, outliers);
        }
        
        return outliers;
        
    }
    
    public void explainContextualOutliers(Context context, OutlierDetector.BatchResult or, DatumEncoder encoder){
    	 if(encoder == null)
    		 return;
    	 FPGrowthEmerging fpg = new FPGrowthEmerging();
    	 double minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
         double minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);
         List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(or.getInliers(),
                                                                         or.getOutliers(),
                                                                         minSupport,
                                                                         minOIRatio,
                                                                         encoder);
         
         log.info("Context: " + context.print(encoder));
     	 log.info("Number of Inliers: " +  or.getInliers().size());
     	 log.info("Number of Outliers: " + or.getOutliers().size());
     	 
     	 log.info("Possible Explanations with minSupport " + minSupport + " and minOIRatio " +  minOIRatio + " : ");
     	 for(ItemsetResult is: isr){
     		StringBuilder sb = new StringBuilder();
     		StringJoiner joiner = new StringJoiner("\n");
     		is.getItems().stream()
                .forEach(i -> joiner.add(String.format("\t%s: %s",
                                                       i.getColumn(),
                                                       i.getValue())));
     		sb.append("\t" + " support: " + is.getSupport() + " oiRation: " + is.getRatioToInliers() + " explanation: " + joiner);
     		log.info(sb.toString());
     	 }
     	 
     	 
     	 /*
     	 //order the isr by their support
         isr.sort(new Comparator<ItemsetResult>(){
			@Override
			public int compare(ItemsetResult o1, ItemsetResult o2) {
				if(o1.getSupport() > o2.getSupport())
					return 1;
				else if(o1.getSupport() < o2.getSupport())
					return -1;
				else 
					return 0;
			}});
         
         
         //order the isr by their OIRatio
         isr.sort(new Comparator<ItemsetResult>(){
 			@Override
 			public int compare(ItemsetResult o1, ItemsetResult o2) {
 				if(o1.getRatioToInliers() > o2.getRatioToInliers())
 					return 1;
 				else if(o1.getRatioToInliers() < o2.getRatioToInliers())
 					return -1;
 				else
 					return 0;
 			}});
          */
         
    }
    
    /**
     * Every context stores its own detector
     * @return
     * @throws ConfigurationException
     */
    private OutlierDetector constructDetector() throws ConfigurationException {
        int metricsDimensions = conf.getStringList(MacroBaseConf.LOW_METRICS).size() + conf.getStringList(MacroBaseConf.HIGH_METRICS).size();

        Long randomSeed = conf.getLong(MacroBaseConf.RANDOM_SEED, MacroBaseDefaults.RANDOM_SEED);

        switch (conf.getDetectorType()) {
            case MAD_OR_MCD:
                if (metricsDimensions == 1) {
                    return new MAD(conf);
                } else {
                    MinCovDet ret = new MinCovDet(conf);
                    if (randomSeed != null) {
                        ret.seedRandom(randomSeed);
                    }
                    return ret;
                }
            case MAD:
                return new MAD(conf);
            case MCD:
                MinCovDet ret = new MinCovDet(conf);
                if (randomSeed != null) {
                    ret.seedRandom(randomSeed);
                }
                return ret;
            case ZSCORE:
                return new ZScore(conf);
            case KDE:
                return new KDE(conf);
            case BINNED_KDE:
                return new BinnedKDE(conf);
            case TREE_KDE:
                return new TreeKDE(conf);
            case MOVING_AVERAGE:
                return new MovingAverage(conf);
            default:
                throw new RuntimeException("Unhandled detector class!" + conf.getDetectorType());
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
			List<Context> denseContexts = initOneDimensionalDenseContextsAndContext2Data(data, dimension, denseContextTau);
			for(Context denseContext: denseContexts){
				ss.addDenseContext(denseContext);
				log.debug(denseContext.toString());
			}
			latticeNodes.add(ss);
		}
		
		return latticeNodes;
	}
	

	private List<LatticeNode> buildOneDimensionalLatticeNodesGivenOutliers(List<Datum> data, List<Datum> inputOutliers){
		//create subspaces
		List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
				
		for(int dimension = 0; dimension < totalContextualDimensions; dimension++){
			LatticeNode ss = new LatticeNode(dimension);
			List<Context> denseContexts = initOneDimensionalDenseContextsAndContext2DataGivenOutliers(data, dimension, inputOutliers);
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
	@Deprecated
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
	
	/**
	 * Initialize one dimensional dense contexts
	 * The number of passes of data is O(totalContextualDimensions)
	 * Store the datums of every one dimensional context in memory
	 * @param data
	 * @param dimension
	 * @return
	 */
	private List<Context> initOneDimensionalDenseContextsAndContext2Data(List<Datum> data, int dimension, double curDensityThreshold){
		int discreteDimensions = contextualDiscreteAttributes.size();
		
		
		List<Context> result = new ArrayList<Context>();
		
		if(dimension < discreteDimensions){
			Map<Integer,List<Integer>> distinctValue2Data = new HashMap<Integer,List<Integer>>();
			for(int i = 0; i < data.size(); i++){
				Datum datum = data.get(i);
				Integer value = datum.getContextualDiscreteAttributes().get(dimension);
				if(distinctValue2Data.containsKey(value)){
					distinctValue2Data.get(value).add(i);
				}else{
					List<Integer> temp = new ArrayList<Integer>();
					temp.add(i);
					distinctValue2Data.put(value, temp);
				}
				
			}
			for(Integer value: distinctValue2Data.keySet()){
				//boolean denseContext = !ContextPruning.densityPruning(data.size(), distinctValue2Count.get(value), denseContextTau);
				boolean denseContext = ( (double) distinctValue2Data.get(value).size() / data.size() >= curDensityThreshold)?true:false;
				if(denseContext){
					Interval interval = new IntervalDiscrete(dimension,contextualDiscreteAttributes.get(dimension),value);
					Context context = new Context(dimension, interval, globalContext);
					result.add(context);
					
					BitSet bs = indexes2BitSet(distinctValue2Data.get(value),data.size());
					context2BitSet.put(context, bs);
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
			HashMap<Interval,List<Integer>> interval2Data = new HashMap<Interval,List<Integer>>();
			for(int i = 0; i < data.size(); i++){
				Datum datum = data.get(i);
				double value = datum.getContextualDoubleAttributes().getEntry(dimension - discreteDimensions );
				for(Interval interval: allIntervals){
					if(interval.contains(value)){
						if(interval2Data.containsKey(interval)){
							interval2Data.get(interval).add(i);
						}else{
							List<Integer> temp = new ArrayList<Integer>();
							temp.add(i);
							interval2Data.put(interval,temp);
						}
						break;
					}
				}
			}
			for(Interval interval: interval2Data.keySet()){
				//boolean denseContext =!ContextPruning.densityPruning(data.size(), interval2Count.get(interval), denseContextTau);
				boolean denseContext = ( (double) interval2Data.get(interval).size() / data.size() >= curDensityThreshold)?true:false;
				if(denseContext){
					Context context = new Context(dimension, interval,globalContext);
					result.add(context);
					
					BitSet bs = indexes2BitSet(interval2Data.get(interval),data.size());
					context2BitSet.put(context, bs);
					
				}
				
				
			}
		}
		
		return result;
	}

	
	private List<Context> initOneDimensionalDenseContextsAndContext2DataGivenOutliers(List<Datum> data, int dimension, List<Datum> inputOutliers){
		List<Context> contextsContainingOutliers = initOneDimensionalDenseContextsAndContext2Data(inputOutliers,dimension, 1.0);
		
		List<Context> result = new ArrayList<Context>();
		
		//re-initialize context2Bitset
		for(Context context: contextsContainingOutliers){
			List<Integer> temp = new ArrayList<Integer>();
			for(int i = 0; i < data.size(); i++){
				Datum datum = data.get(i);
				if(context.containDatum(datum)){
					temp.add(i);
				}
			}
			boolean denseContext = ( (double) temp.size() / data.size() >= denseContextTau)?true:false;
			if(denseContext){
				BitSet bs = indexes2BitSet(temp,data.size());
				context2BitSet.put(context, bs);
				result.add(context);
			}
			
		}
		return result;
	}
	
	
	//trade memory for efficiency
	//private Map<Context,HashSet<Datum>> context2Data = new HashMap<Context,HashSet<Datum>>();
	private Map<Context,BitSet> context2BitSet = new HashMap<Context,BitSet>();
	
	private BitSet indexes2BitSet(List<Integer> indexes, int total){
		BitSet bs = new BitSet(total);
		for(int i = 0; i < indexes.size(); i++){
			int index = indexes.get(i);
			bs.set(index);
		}
		return bs;
	}
	private List<Integer> bitSet2Indexes(BitSet bs){
		List<Integer> indexes = new ArrayList<Integer>();
		for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
		     // operate on index i here
			indexes.add(i);
		     if (i == Integer.MAX_VALUE) {
		         break; // or (i+1) would overflow
		     }
		}
		return indexes;
	}
    
	
	
}

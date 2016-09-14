package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.io.*;

import org.apache.commons.math3.stat.inference.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.classify.StaticThresholdClassifier;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.MAD;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseConf.TransformType;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.util.BitSetUtil;
import macrobase.util.HypothesisTesting;
import macrobase.util.MemoryUtil;

public class ContextualOutlierDetector {
    private static final Logger log = LoggerFactory.getLogger(ContextualOutlierDetector.class);

    private MacroBaseConf conf;
    private List<String> contextualDiscreteAttributes;
    private List<String> contextualDoubleAttributes;
    private int totalContextualDimensions;
    private Context globalContext;
    private double denseContextTau;
    private int numIntervals;
    private int maxPredicates;
    private DatumEncoder encoder;
    private String contextualOutputFile;

    //The following are context pruning options
    //This is the outliers detected for every dense context
    private List<Context> contextWithOutliers = new ArrayList<Context>();

    public ContextualOutlierDetector(MacroBaseConf conf) throws IOException {
        this.conf = conf;
        this.contextualDiscreteAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES,
                                                               MacroBaseDefaults.CONTEXTUAL_DISCRETE_ATTRIBUTES);
        this.contextualDoubleAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES,
                                                             MacroBaseDefaults.CONTEXTUAL_DOUBLE_ATTRIBUTES);
        this.denseContextTau = conf.getDouble(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU,
                                              MacroBaseDefaults.CONTEXTUAL_DENSECONTEXTTAU);
        this.numIntervals = conf.getInt(MacroBaseConf.CONTEXTUAL_NUMINTERVALS,
                                        MacroBaseDefaults.CONTEXTUAL_NUMINTERVALS);
        this.maxPredicates = conf.getInt(MacroBaseConf.CONTEXTUAL_MAX_PREDICATES,
                                         MacroBaseDefaults.CONTEXTUAL_MAX_PREDICATES);
        this.totalContextualDimensions = contextualDiscreteAttributes.size() + contextualDoubleAttributes.size();
        this.encoder = conf.getEncoder();
        this.contextualOutputFile = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,
                                                     MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE);      
        log.debug("There are {} contextualDiscreteAttributes, and {} contextualDoubleAttributes",
                  contextualDiscreteAttributes.size(), contextualDoubleAttributes.size());
        
    }
    
    
    public void writeAllContextualOutliers() throws IOException {
        if (contextualOutputFile == null)
            return;
         
        PrintWriter contextualOut = new PrintWriter(new FileWriter(contextualOutputFile,true));
        for (Context context: contextWithOutliers) {
            contextualOut.println("Context: " + context.print(conf.getEncoder()));
            contextualOut.println("\t Number of inliners " + (context.getBitSet().cardinality() - context.getOutlierBitSet().cardinality()));
            contextualOut.println("\t Number of outliers " + context.getOutlierBitSet().cardinality());
            contextualOut.println("\t " + context.getBitSet().toString());
            contextualOut.println("\t " + context.getOutlierBitSet().toString());
        }
        contextualOut.close();
        
        
        //analyze the contexts that have outliers found, number of dimensions
        PrintWriter contextualOutAnalysis = new PrintWriter(new FileWriter(contextualOutputFile + "_analysis.txt",true));
        contextualOutAnalysis.println("Total number of contexts that have outliers not contained in parents: " + contextWithOutliers.size());
        
        Map<Integer, Integer> numDim2Count = new HashMap<Integer, Integer>();
        for (Context context: contextWithOutliers) {
            int numDim = context.getDimensions().size();
            if (numDim2Count.containsKey(numDim)) {
                numDim2Count.put(numDim, numDim2Count.get(numDim) + 1);
            } else {
                numDim2Count.put(numDim, 1);
            }
        }
        for (int i = 0; i < 10; i++) {
            contextualOutAnalysis.println("Total number of contexts that have " + i + " dimensions: " + numDim2Count.get(i));
        }
        
        
        Map<Context, Integer> oneDimContexts2Count = new HashMap<Context, Integer>();
        for (Context context: contextWithOutliers) {
            for (Context oneDimContext : context.getOneDimensionalAncestors() ){
                if (oneDimContexts2Count.containsKey(oneDimContext)) {
                    oneDimContexts2Count.put(oneDimContext, oneDimContexts2Count.get(oneDimContext) + 1);
                } else {
                    oneDimContexts2Count.put(oneDimContext, 1);
                }
            }
        }
        for (Context oneDimContext: oneDimContexts2Count.keySet()) {
            contextualOutAnalysis.println("One Dimensional context " + oneDimContext.print(encoder) + " particiates in contexts with outliers: " + oneDimContexts2Count.get(oneDimContext));
        }
        
        contextualOutAnalysis.close();
           
    }

    public void searchContextualOutliersDataDrivenDebug(List<Datum> data, String debug_file) throws IOException, ConfigurationException {
        
        PrintWriter debugOutput = new PrintWriter(new FileWriter(debug_file));
        List<Interval> intervals = new Discretization(data).clusterOneDimensionalValues(conf);

        Map<Interval, List<Datum>> interval2Data = new HashMap<Interval, List<Datum>>();
        for (Interval interval: intervals) {
            interval2Data.put(interval, new ArrayList<Datum>());
        }
        for (Interval interval: intervals) {
            for (Datum d: data) {
                if (interval.contains(d.getMetrics().getEntry(0))) {
                    interval2Data.get(interval).add(d);
                }
            }
        }
        
       
        int numCaptured = 0;
        int numMissed = 0;
        for (Context context: contextWithOutliers) {
            //step1: get interval 2 count
            Map<Interval, Integer> interval2Count = new HashMap<Interval, Integer>();
            for (Interval interval: intervals) {
                interval2Count.put(interval, 0);
            }
            
            BitSet bs = context.getBitSet();
            List<Integer> indexes = BitSetUtil.bitSet2Indexes(bs);
            for (Interval interval: intervals) {
                for (Integer index: indexes) {
                    if (interval.contains(data.get(index).getMetrics().getEntry(0))) {
                        interval2Count.put(interval, interval2Count.get(interval) + 1);
                    }
                }
            }
            
            boolean captured = false;
            for (Interval interval: intervals) {
                double contextInIntervalRatio = (double)interval2Count.get(interval) / context.getBitSet().cardinality();
                if (contextInIntervalRatio >= conf.getDouble(MacroBaseConf.CONTEXTUAL_DATADRIVEN_THRESHOLD, MacroBaseDefaults.CONTEXTUAL_DATADRIVEN_THRESHOLD)) {
                    captured = true;
                    break;
                }
            }
            if (captured) {
                numCaptured++;
            } else {
                numMissed++;
            }
            
            debugOutput.println("**************");
            debugOutput.println("Context: " + context.print(encoder) + " number of tuples: " + 
            context.getBitSet().cardinality() + " number of outliers: " +
                    context.getOutlierBitSet().cardinality()
                    + "\t Captured ? " + captured
                    //+ "\t Failed at count? " + failedAtCount
                    );
            
           
            for (Interval interval: intervals) {
                debugOutput.println(interval.toString() + "\t" + interval2Count.get(interval));
            }
            
            debugOutput.println("**************");
            
           
        }
        System.err.println("Data Driven Debug: Number of contexts with outliers: " + contextWithOutliers.size());
        System.err.println("Data Driven Debug: Number of captured: " + numCaptured);
        System.err.println("Data Driven Debug: Number of missed: " + numMissed);

        debugOutput.close();
        
    }
    
    public List<Context> searchContextualOutliersDataDriven_NewApproach(List<Datum> data) throws Exception {
        log.debug("Data Driven Approach, clustering the metric attributes:");
        
        
        globalContext = new Context(data, conf);
        contextualOutlierDetection(data, globalContext);
        
        
        List<BitSet> clustersBS = new ArrayList<BitSet>();
        //1. every tuple is in its own cluster
        //2. tuples with the same metric attribute are in the same cluster
        //3. tuples with similar metric attribute are in the same cluster
        
        for (int i = 0; i < data.size(); i++) {
            BitSet clusterBS = new BitSet(data.size());
            clusterBS.set(i);
            clustersBS.add(clusterBS);    
        }
        /*
        BitSet clusterBSTemp = new BitSet(data.size());
        clusterBSTemp.set(1);
        clustersBS.add(clusterBSTemp);   
        */
        List<LatticeNode> preLatticeNodes = new ArrayList<LatticeNode>();
        List<LatticeNode> curLatticeNodes = new ArrayList<LatticeNode>();
        for (int level = 1; level <= totalContextualDimensions; level++) {
            if (level > maxPredicates)
                break;
            log.debug("Lattice level {}", level);
            long timeBuildLatticeBefore = System.currentTimeMillis();
            if (level == 1) {
                curLatticeNodes = buildOneDimensionalLatticeNodes(data);
                int numberOfPredicates = 0;
                for (LatticeNode node : curLatticeNodes) {
                    for (Context context : node.getDenseContexts()) {
                        numberOfPredicates++;
                        boolean[] dataDrivenPruned = new boolean[clustersBS.size()];
                        for (int i = 0; i < clustersBS.size(); i++)
                            dataDrivenPruned[i] = false;
                        context.setDataDrivenPruned(dataDrivenPruned);
                    }
                }
                log.debug("Number of predicates {}", numberOfPredicates);
            } else {
                curLatticeNodes = levelUpLattice(preLatticeNodes, data);
            }
            long timeBuildLatticeAfter = System.currentTimeMillis();
            ContextStats.timeBuildLattice += (timeBuildLatticeAfter - timeBuildLatticeBefore);
            if (curLatticeNodes.size() == 0) {
                break;
            }
            //run contextual outlier detection
            for (LatticeNode node : curLatticeNodes) {
                HashSet<Context> toBeRemoved = new HashSet<Context>();
                for (Context context : node.getDenseContexts()) {
                    if (context.getDataDrivenPrunedAll()) {
                        toBeRemoved.add(context);
                        ContextStats.numDataDrivenContextsPruning++;
                        continue;
                    }
                    for (int c = 0; c < clustersBS.size(); c++) {
                        if (context.getDataDrivenPruned(c) == false) {
                            //if this context does not contain any tuples from a cluster, the prune
                            BitSet clusterBS = clustersBS.get(c);
                            if (BitSetUtil.subSetCoverage(context.getBitSet(), clusterBS) == 0) {
                                context.setDataDrivenPruned(c);
                            }
                        }                       
                    }
                    if (context.getDataDrivenPrunedAll()) {
                        toBeRemoved.add(context);
                        ContextStats.numDataDrivenContextsPruning++;
                        continue;
                    }
                    List<Datum> outliers = contextualOutlierDetection(data, context);
                    BitSet outlierBS = context.getOutlierBitSet();
                    //if the outliers contain tuples from a cluster, then the context can be pruned w.r.t. that cluster
                    if (outliers != null && outliers.size() > 0 )  {
                        contextWithOutliers.add(context);
                        for (int c = 0; c < clustersBS.size(); c++) {
                            if (context.getDataDrivenPruned(c) == false) {
                                BitSet clusterBS = clustersBS.get(c);
                                for (int i = clusterBS.nextSetBit(0); i >= 0; i = clusterBS.nextSetBit(i + 1)) {
                                    if (i == Integer.MAX_VALUE) {
                                        break; // or (i+1) would overflow
                                    }
                                    if (outlierBS.get(i)) {
                                        context.setDataDrivenPruned(c);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (context.getDataDrivenPrunedAll()) {
                        ContextStats.numDataDrivenContextsPruning++;
                        toBeRemoved.add(context);
                        continue;
                    }
                    
                }
                node.getDenseContexts().removeAll(toBeRemoved);
            }
            preLatticeNodes = curLatticeNodes;
        }
        return contextWithOutliers;
    }
    
    /**
     * Two different interfaces, search all contextual outliers, whether given outliers or not
     *
     * @param data
     * @throws Exception
     */
    public List<Context> searchContextualOutliers(List<Datum> data) throws Exception {
        List<Datum> inputOutliers = new ArrayList<Datum>();
        
        String suspiciousTupleIndexes = conf.getString(MacroBaseConf.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX,
                                                               MacroBaseDefaults.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX);
        if (suspiciousTupleIndexes.equals(MacroBaseDefaults.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX)) {
            //default, all contextual outliers
            return searchContextGivenOutliers(data, inputOutliers);
        } else {
            String[] indexes = suspiciousTupleIndexes.split(",");
            for (String index: indexes) {
                inputOutliers.add(data.get(Integer.valueOf(index)));
            }
            return searchContextGivenOutliers(data, inputOutliers);
        }
             
    }

    public List<Context> searchContextGivenOutliers(List<Datum> data, List<Datum> inputOutliers) throws Exception {
        //result contexts that have the input outliers
        
        globalContext = new Context(data, conf);
        List<Datum> globalOutliers = contextualOutlierDetection(data, globalContext);
        if (globalOutliers != null && globalOutliers.size() > 0 && globalOutliers.containsAll(inputOutliers)) {
            contextWithOutliers.add(globalContext);
        }
        
        List<LatticeNode> preLatticeNodes = new ArrayList<LatticeNode>();
        List<LatticeNode> curLatticeNodes = new ArrayList<LatticeNode>();
        for (int level = 1; level <= totalContextualDimensions; level++) {
            if (level > maxPredicates)
                break;
            log.debug("Lattice level {}", level);
            long timeBuildLatticeBefore = System.currentTimeMillis();
            if (level == 1) {
                curLatticeNodes = (inputOutliers.size() > 0 ) ? 
                        buildOneDimensionalLatticeNodesGivenOutliers(data, inputOutliers):
                            buildOneDimensionalLatticeNodes(data);
                int numberOfPredicates = 0;
                for (LatticeNode node : curLatticeNodes) {
                    for (Context context : node.getDenseContexts()) {
                        numberOfPredicates++;
                    }
                }
                log.debug("Number of predicates {}", numberOfPredicates);
            } else {
                curLatticeNodes = levelUpLattice(preLatticeNodes, data);
            }
            long timeBuildLatticeAfter = System.currentTimeMillis();
            ContextStats.timeBuildLattice += (timeBuildLatticeAfter - timeBuildLatticeBefore);
            if (curLatticeNodes.size() == 0) {
                break;
            }
            //run contextual outlier detection
            for (LatticeNode node : curLatticeNodes) {
                HashSet<Context> toBeRemoved = new HashSet<Context>();
                for (Context context : node.getDenseContexts()) {
                    List<Datum> outliers = contextualOutlierDetection(data, context);
                    if (outliers != null && outliers.size() > 0 && outliers.containsAll(inputOutliers)) {
                        contextWithOutliers.add(context);
                        if (inputOutliers.size() > 0) {
                            //this is the second interface, given outliers
                            toBeRemoved.add(context);
                        }
                    }
                }
                node.getDenseContexts().removeAll(toBeRemoved);
            }
            preLatticeNodes = curLatticeNodes;
        }
        //writeAllContextualOutliers();
        return contextWithOutliers;
    }

    
    
    private ContextIndexTree cit;
    /**
     * Walking up the lattice, construct the lattice node, when include those lattice nodes that contain at least one dense context
     *
     * @param latticeNodes
     * @param data
     * @return
     */
    private List<LatticeNode> levelUpLattice(List<LatticeNode> latticeNodes, List<Datum> data) {
        if (latticeNodes.get(0).getDimensions().size() >= 1) {
            //build the one-dimensional cit
            cit = new ContextIndexTree();
            for (LatticeNode latticeNode: latticeNodes) {
                for (Context c: latticeNode.getDenseContexts()) {
                    cit.addContext(c);
                }
            }
        }
      
        List<LatticeNode> latticeNodeByDimensions = new ArrayList<LatticeNode>(latticeNodes);
        Collections.sort(latticeNodeByDimensions, new LatticeNode.DimensionComparator());
      
        //find out dense candidate subspaces
        List<LatticeNode> result = new ArrayList<LatticeNode>();
   
        for (int i = 0; i < latticeNodeByDimensions.size(); i++) {
            for (int j = i + 1; j < latticeNodeByDimensions.size(); j++) {
                LatticeNode s1 = latticeNodeByDimensions.get(i);
                LatticeNode s2 = latticeNodeByDimensions.get(j);
                LatticeNode joined = s1.join(s2, data, denseContextTau, cit);
                if (joined != null) {
                    //only interested in nodes that have dense contexts
                    if (joined.getDenseContexts().size() != 0) {
                        result.add(joined);
                    }
                }
            }
        }
        return result;
    }

        
    /**
     * Run outlier detection algorithm on contextual data
     * The algorithm has to static threhold classifier
     *
     * @param data
     * @param context
     * @return
     * @throws Exception
     */
    public List<Datum> contextualOutlierDetection(List<Datum> data, Context context) throws Exception {
        
        //visit the context
        ContextStats.numContextsGenerated++;
        
        long timeDetectContextualOutliersBefore = System.currentTimeMillis();
        
        //This has to be done regardless of using distribution pruning or not
        List<Datum> contextualData = new ArrayList<Datum>();
        BitSet bs = context.getBitSet();
        List<Integer> indexes = BitSetUtil.bitSet2Indexes(bs);
        for (int i = 0; i < indexes.size(); i++) {
            Datum curDatum = data.get(indexes.get(i));
            contextualData.add(curDatum);
        }
        
        if (conf.getBoolean(MacroBaseConf.CONTEXTUAL_PRUNING_SUBSUMPTION, MacroBaseDefaults.CONTEXTUAL_PRUNING_SUBSUMPTION)
                && conf.getTransformType() == TransformType.MAD
               ) {
            boolean MADSpecificPruned = MADSpecificPruning(context, contextualData);
            if (MADSpecificPruned) {
                ContextStats.numSubsumptionPruning++;
                return null;
            }
        }
        
        
        //prepare to do outlier detection in this context
        context.setDetector(constructDetector());
       
        //perform outlier detection on this context
        FeatureTransform featureTransform = new BatchScoreFeatureTransform(context.getDetector(), true);
        featureTransform.consume(contextualData);
        OutlierClassifier outlierClassifier = new StaticThresholdClassifier(conf);
        outlierClassifier.consume(featureTransform.getStream().drain());
        List<OutlierClassificationResult> outlierClassificationResults = outlierClassifier.getStream().drain();
        List<Integer> outlierIndexes = new ArrayList<Integer>();
        for (int i = 0; i < outlierClassificationResults.size(); i++) {
            OutlierClassificationResult outlierClassificationResult = outlierClassificationResults.get(i);
            if (outlierClassificationResult.isOutlier()) {
                outlierIndexes.add(indexes.get(i));    
            }
        }
        context.setMedian(((MAD)((BatchScoreFeatureTransform)featureTransform).getBatchTrainScore()).getMedian());
        context.setMAD(((MAD)((BatchScoreFeatureTransform)featureTransform).getBatchTrainScore()).getMAD());
        
        //is there outliers in this context
        //construct outliers from outlierIndexes
        List<Datum> outliers = new ArrayList<Datum>();
        if (outlierIndexes.size() > 0) {
            ContextStats.numContextsGeneratedWithOutliers++;
            BitSet outlierBitSet = BitSetUtil.indexes2BitSet(outlierIndexes, data.size());
            context.setOutlierBitSet(outlierBitSet);
            int numberNewOutliers = numberOutliersNotContainedInAncestor(context);
            //boolean contained = contextualOutliersContained(context, 1);
            if (numberNewOutliers > 0* outlierBitSet.cardinality()) {
                context.setNumberOfOutliersNotContainedInAncestor(numberNewOutliers);
                //outliers not contained in parent context outliers
                for (int i = 0; i < outlierIndexes.size(); i++) {
                    outliers.add(data.get(i));
                }
                ContextStats.numContextsGeneratedWithMaximalOutliers++;  
            } 
        } else {
            ContextStats.numContextsGeneratedWithOutOutliers++;
        }
        
        long timeDetectContextualOutliersAfter = System.currentTimeMillis();
        ContextStats.timeDetectContextualOutliers += (timeDetectContextualOutliersAfter - timeDetectContextualOutliersBefore);
        return outliers;
    }
    
   
    private boolean MADSpecificPruning(Context context, List<Datum> contextualData) {
        double[] metrics = new double[contextualData.size()];
        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = contextualData.get(i).getMetrics().getEntry(0);
        }
        
        for (Context parent: context.getParents()) {
            //does context have the same median and MAD as the parent
            double parentMedian = parent.getMedian();
            double parentMAD = parent.getMAD();
            
            int negMedian = 0;
            int posMedian = 0;
            int exactMedian = 0;
            for (double metric: metrics) {
                if (metric < parentMedian) {
                    negMedian++;
                } else if (metric > parentMedian) {
                    posMedian++;
                } else {
                    exactMedian++;
                }
            }
            boolean sameMedian = MADPruningSpecificHelper(negMedian, posMedian, exactMedian); 
            if (!sameMedian) 
                continue;
            
            
            int negMAD = 0;
            int posMAD = 0;
            int exactMAD = 0;
            for (double metric: metrics) {
                double value = Math.abs(metric - parentMedian);
                if (value < parentMAD) {
                    negMAD++;
                } else if (value > parentMAD) {
                    posMAD++;
                } else {
                    exactMAD++;
                }
            }
            boolean sameMAD = MADPruningSpecificHelper(negMAD, posMAD ,exactMAD);
            if (!sameMAD)
                continue;
            
            //System.err.println("MAD Specific Pruning, parent median: " + parentMedian + " parent mad: " + parentMAD);
            //we have a parent with same median and MAD
            context.setMedian(parentMedian);
            context.setMAD(parentMAD);
            if (parent.getOutlierBitSet() != null) {
                BitSet outlierBitSet = (BitSet)parent.getOutlierBitSet().clone();
                outlierBitSet.and(context.getBitSet());
                context.setOutlierBitSet(outlierBitSet);
            }
                
            return true;
        }
        return false;
    }
    
    private boolean MADPruningSpecificHelper(int negMedian, int posMedian, int exactMedian)  {
        if (posMedian == negMedian) {
            return true;
        } else if (posMedian > negMedian) {
            if (negMedian + exactMedian > posMedian) {
                return true;
            } else {
                //approximate
            }
        } else {
            if (posMedian + exactMedian > negMedian) {
                return true;
            } else {
                //approximate
            }
        }
        return false;
    }
    
    /**
     * Return the parent context whose outliers contain the outliers in context
     * @param context
     * @param threshold
     * @return
     */
    private boolean contextualOutliersContained(Context context, double threshold) {
        BitSet outlierBitSet = context.getOutlierBitSet();
        BitSet ancestorOutlierBitSet = context.getAncestorOutlierBitSet();
        
        if (ancestorOutlierBitSet == null) {
            //global context
            return false;
        }
       
        double coverage = BitSetUtil.subSetCoverage(outlierBitSet, ancestorOutlierBitSet);
        if (coverage >= threshold) {
            return true;
        } else {
            return false;
        }
       
    }
    
    private int numberOutliersNotContainedInAncestor(Context context) {
        BitSet outlierBitSet = context.getOutlierBitSet();
        BitSet ancestorOutlierBitSet = context.getAncestorOutlierBitSet();
        
        if (ancestorOutlierBitSet == null) {
            return outlierBitSet.cardinality();
        }
        if (outlierBitSet.cardinality() == 0) {
            return 0;
         }
         BitSet bsClone = (BitSet)outlierBitSet.clone();
         bsClone.andNot(ancestorOutlierBitSet);
         return bsClone.cardinality();
    }
    


    /**
     * Every context stores its own detector
     *
     * @return
     * @throws ConfigurationException
     */
    private BatchTrainScore constructDetector() throws ConfigurationException {
        return conf.constructTransform(conf.getTransformType());
    }

    /**
     * Find one dimensional lattice nodes with dense contexts
     *
     * @param data
     * @return
     * @throws ConfigurationException 
     */
    private List<LatticeNode> buildOneDimensionalLatticeNodes(List<Datum> data) throws ConfigurationException {
        //create subspaces
        List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
        for (int dimension = 0; dimension < totalContextualDimensions; dimension++) {
            LatticeNode ss = new LatticeNode(dimension);
            List<Context> denseContexts = initOneDimensionalDenseContextsAndContext2Data(data, dimension,
                                                                                         denseContextTau);            
            for (Context denseContext : denseContexts) {
                ss.addDenseContext(denseContext);
            }
            latticeNodes.add(ss);
        }
        return latticeNodes;
    }

    private List<LatticeNode> buildOneDimensionalLatticeNodesGivenOutliers(List<Datum> data, List<Datum> inputOutliers) throws ConfigurationException {
        //create subspaces
        List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
        for (int dimension = 0; dimension < totalContextualDimensions; dimension++) {
            LatticeNode ss = new LatticeNode(dimension);
            List<Context> denseContexts = initOneDimensionalDenseContextsAndContext2DataGivenOutliers(data, dimension,
                                                                                                      inputOutliers);
            for (Context denseContext : denseContexts) {
                ss.addDenseContext(denseContext);
                if (isEncoderSetup())
                    log.debug(denseContext.toString() + " ---- " + denseContext.print(encoder));
                else
                    log.debug(denseContext.toString());
            }
            latticeNodes.add(ss);
        }
        return latticeNodes;
    }

    private boolean isEncoderSetup() {
        if (encoder == null)
            return false;
        if (encoder.getNextKey() == 0)
            return false;
        return true;
    }
    public DatumEncoder getEncoder() {
        return encoder;
    }

    /**
     * Does this interval worth considering
     * A = null is not worth considering
     *
     * @param interval
     * @return
     */
    private boolean isInterestingInterval(Interval interval) {
        if (isEncoderSetup() == false)
            return true;
        if (interval instanceof IntervalDiscrete) {
            IntervalDiscrete id = (IntervalDiscrete) interval;
            String columnValue = encoder.getAttribute(id.getValue()).getValue();
            if (columnValue == null || columnValue.equals("null")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Initialize one dimensional dense contexts
     * The number of passes of data is O(totalContextualDimensions)
     * Store the datums of every one dimensional context in memory
     *
     * @param data
     * @param dimension
     * @return
     * @throws ConfigurationException 
     */
    private List<Context> initOneDimensionalDenseContextsAndContext2Data(List<Datum> data, int dimension, double curDensityThreshold) throws ConfigurationException {
        int discreteDimensions = contextualDiscreteAttributes.size();
        List<Context> result = new ArrayList<Context>();
        if (dimension < discreteDimensions) {
            log.debug("Building one dimensional contexts for discrete attribute: {}",
                    contextualDiscreteAttributes.get(dimension));
            
            Map<Integer, List<Integer>> distinctValue2Data = new HashMap<Integer, List<Integer>>();
            for (int i = 0; i < data.size(); i++) {
                Datum datum = data.get(i);
                Integer value = datum.getContextualDiscreteAttributes().get(dimension);
                if (distinctValue2Data.containsKey(value)) {
                    distinctValue2Data.get(value).add(i);
                } else {
                    List<Integer> temp = new ArrayList<Integer>();
                    temp.add(i);
                    distinctValue2Data.put(value, temp);
                }
            }
            for (Integer value : distinctValue2Data.keySet()) {
                boolean denseContext = ((double) distinctValue2Data.get(
                        value).size() / data.size() >= curDensityThreshold) ? true : false;
                if (denseContext) {
                    Interval interval = new IntervalDiscrete(dimension, contextualDiscreteAttributes.get(dimension),
                                                             value);
                    if (isInterestingInterval(interval)) {
                        Context context = new Context(dimension, interval, globalContext);
                        result.add(context);
                        BitSet bs = BitSetUtil.indexes2BitSet(distinctValue2Data.get(value), data.size());
                        context.setBitSet(bs);
                        
                        if (isEncoderSetup())
                            log.debug("\t" + context.toString() + " ---- " + context.print(encoder) + " ---- " + context.getBitSet().cardinality());
                        else
                            log.debug("\t" +context.toString());
                    }
                }
            }
        } else {
            log.debug("Building one dimensional contexts for double attribute: {}",
                    contextualDoubleAttributes.get(dimension-discreteDimensions));
            double[] values = new double[data.size()];
            HashSet<Double> distinctValues = new HashSet<Double>();
            //find out the min, max
            for (int i = 0; i < data.size(); i++) {
                Datum datum = data.get(i);
                double value = datum.getContextualDoubleAttributes().getEntry(dimension - discreteDimensions);
                values[i] = value;
                distinctValues.add(value);
            }
            if (distinctValues.size() == 1) {
                log.debug("\t only one value in this attribute value is: {}", values[0]);
                return result;
            }
                
          
            HashSet<Interval> allIntervals = new HashSet<Interval>();
            List<Interval> tempIntervals = new Discretization(values).kMeans(20);
            for (Interval tempInterval: tempIntervals) {
                Interval interval = new IntervalDouble(dimension, contextualDoubleAttributes.get(
                            dimension - discreteDimensions), ((IntervalDouble)tempInterval).getMin(), ((IntervalDouble)tempInterval).getMax());
                allIntervals.add(interval);
            }
            
            //count the interval
            HashMap<Interval, List<Integer>> interval2Data = new HashMap<Interval, List<Integer>>();
            for (int i = 0; i < data.size(); i++) {
                Datum datum = data.get(i);
                double value = datum.getContextualDoubleAttributes().getEntry(dimension - discreteDimensions);
                for (Interval interval : allIntervals) {
                    if (interval.contains(value)) {
                        if (interval2Data.containsKey(interval)) {
                            interval2Data.get(interval).add(i);
                        } else {
                            List<Integer> temp = new ArrayList<Integer>();
                            temp.add(i);
                            interval2Data.put(interval, temp);
                        }
                        break;
                    }
                }
            }
            for (Interval interval : interval2Data.keySet()) {
                boolean denseContext = ((double) interval2Data.get(
                        interval).size() / data.size() >= curDensityThreshold) ? true : false;
                if (denseContext) {
                    if (isInterestingInterval(interval)) {
                        Context context = new Context(dimension, interval, globalContext);
                        result.add(context);
                        BitSet bs = BitSetUtil.indexes2BitSet(interval2Data.get(interval), data.size());
                        context.setBitSet(bs);
                        
                        if (isEncoderSetup())
                            log.debug("\t" + context.toString() + " ---- " + context.print(encoder) + " ---- " + context.getBitSet().cardinality());
                        else
                            log.debug("\t" +context.toString());
                    }
                }
            }
        }
        return result;
    }

    private List<Context> initOneDimensionalDenseContextsAndContext2DataGivenOutliers(List<Datum> data, int dimension, List<Datum> inputOutliers) throws ConfigurationException {
        List<Context> contextsContainingOutliers = initOneDimensionalDenseContextsAndContext2Data(inputOutliers,
                                                                                                  dimension, 1.0);
        List<Context> result = new ArrayList<Context>();
        //re-initialize context2Bitset
        for (Context context : contextsContainingOutliers) {
            List<Integer> temp = new ArrayList<Integer>();
            for (int i = 0; i < data.size(); i++) {
                Datum datum = data.get(i);
                if (context.containDatum(datum)) {
                    temp.add(i);
                }
            }
            boolean denseContext = ((double) temp.size() / data.size() >= denseContextTau) ? true : false;
            if (denseContext) {
                BitSet bs = BitSetUtil.indexes2BitSet(temp, data.size());
                context.setBitSet(bs);
                result.add(context);
            }
        }
        return result;
    }



}

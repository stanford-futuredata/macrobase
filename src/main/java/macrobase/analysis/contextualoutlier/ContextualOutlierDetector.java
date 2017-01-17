package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.classify.StaticThresholdClassifier;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.MAD;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseConf.TransformType;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.util.BitSetUtil;

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
    
    private List<LatticeNode> oneDimensionalLatticeNodes = null;

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
    
    public ContextualOutlierDetector(MacroBaseConf conf, List<LatticeNode> oneDimensionalLatticeNodes) throws IOException {
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
        
        this.oneDimensionalLatticeNodes = oneDimensionalLatticeNodes;
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
    
    /**
     * Two different interfaces, search all contextual outliers, whether given outliers or not
     *
     * @param data
     * @throws Exception
     */
    public List<Context> searchContextualOutliers(List<Datum> data) throws Exception {
        return searchContextGivenOutliers(data, new ArrayList<Datum>());
    }

    /**
     * Return true if foundOutliers contains at least one inputOutliers
     * @param foundOutliers
     * @param inputOutliers
     * @return
     */
    private boolean containAtLeastOneInputOutliers(List<Datum> foundOutliers, List<Datum> inputOutliers) {
        if (inputOutliers.size() == 0)
            return true;
        
        for (Datum d : inputOutliers) {
            if (foundOutliers.contains(d))
                return true;
        }
        return false;
    }
    
    /**
     * This context can be pruned if for all the tuples in inputOutlierIndexes that it does contain, they have been declared as outliers
     * @param context
     * @param inputOutlierIndexes
     * @return
     */
    private boolean contextPruningInverse(Context context, List<Integer> inputOutlierIndexes) {
        List<Integer> containedInputOutlierIndexes = new ArrayList<Integer>();
        
        List<Integer> is = BitSetUtil.bitSet2Indexes(context.getInverseBitSet());
        for (Integer i: is) {
            containedInputOutlierIndexes.add(inputOutlierIndexes.get(i));
        }
        
        for (Integer index: containedInputOutlierIndexes) {
            boolean isOutlierInSuperContexts = false;
            if (context.getAncestorOutlierBitSet().get(index)) 
                isOutlierInSuperContexts = true;
            if (context.getOutlierBitSet() != null && context.getOutlierBitSet().get(index)) 
                isOutlierInSuperContexts = true;
            if (isOutlierInSuperContexts == false) {
                return false;
            }
        }
        ContextStats.numInversePruningInputOutliersContained++;
        return true;
    }
    
    public List<Context> searchContextGivenOutliers(List<Datum> data, List<Datum> inputOutliers) throws Exception {
        List<Integer> inputOutlierIndexes = new ArrayList<Integer>();
        if (inputOutliers.size() > 0) {
            for (Datum d: inputOutliers) {
                int inputOutlierIndex = data.indexOf(d);
                inputOutlierIndexes.add(inputOutlierIndex);
            }
            
        }
        
        //result contexts that have the input outliers
        globalContext = new Context(data, conf);
        if (inputOutliers.size() > 0) {
            BitSet inverseBitSet = new BitSet(inputOutliers.size());
            inverseBitSet.set(0, inputOutliers.size());
            globalContext.setInverseBitSet(inverseBitSet);
        }
        List<Datum> globalOutliers = contextualOutlierDetection(data, globalContext, inputOutlierIndexes);
        if (globalOutliers != null && globalOutliers.size() > 0 && containAtLeastOneInputOutliers(globalOutliers, inputOutliers)) {
            contextWithOutliers.add(globalContext);
        }
        //The inverse problem can terminate, since global context contains all inputOutliers
        if (inputOutliers.size() > 0 && globalOutliers != null && globalOutliers.size() > 0 && globalOutliers.containsAll(inputOutliers)) {
            return contextWithOutliers;
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
                        buildOneDimensionalLatticeNodesGivenOutliers(data, inputOutliers, inputOutlierIndexes):
                            buildOneDimensionalLatticeNodes(data);
                int numberOfPredicates = 0;
                for (LatticeNode node : curLatticeNodes) {
                    numberOfPredicates += node.getDenseContexts().size();  
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
                    List<Datum> maximalOutliers = contextualOutlierDetection(data, context, inputOutlierIndexes);
                    if (maximalOutliers != null && maximalOutliers.size() > 0 && containAtLeastOneInputOutliers(maximalOutliers, inputOutliers)) {
                        contextWithOutliers.add(context);
                    }
                    if (inputOutliers.size() > 0 && contextPruningInverse(context, inputOutlierIndexes)) {
                        //this is the second interface, given outliers
                        toBeRemoved.add(context);
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
        List<LatticeNode> result = new ArrayList<LatticeNode>();
        
        LatticeNodeIndexTree lit = new LatticeNodeIndexTree();
        for (LatticeNode ln: latticeNodeByDimensions) {
            lit.addLatticeNode(ln);
        }
        
        /*
        for (int i = 0; i < latticeNodeByDimensions.size(); i++) {
            
            List<LatticeNode> test1 = new ArrayList<LatticeNode>();
            LatticeNode s1 = latticeNodeByDimensions.get(i);
            
            List<Integer> s1Dimensions = new ArrayList<Integer>(s1.getDimensions());
            int k = s1Dimensions.get(s1Dimensions.size() - 1) + 1;
            for ( ; k < totalContextualDimensions; k++) {
                s1Dimensions.remove(s1Dimensions.size() - 1);
                s1Dimensions.add(k);
                LatticeNode s2 = lit.getLatticeNode(s1Dimensions);
                if (s2 != null) {
                    LatticeNode joined = s1.join(s2, data, denseContextTau, cit);
                    if (joined != null) {
                        test1.add(s2);
                    }
                }
                
            }
            
            
            List<LatticeNode> test2 = new ArrayList<LatticeNode>();
            for (int j = i + 1; j < latticeNodeByDimensions.size(); j++) {
                LatticeNode s2 = latticeNodeByDimensions.get(j);
                LatticeNode joined = s1.join(s2, data, denseContextTau, cit);
                if (joined != null) {
                   test2.add(s2);
                }
            }
            
            if (!test1.containsAll(test2) || !test2.containsAll(test1)) {
                System.err.println("ERROR");
            }
        }
        */
        
        
        for (int i = 0; i < latticeNodeByDimensions.size(); i++) {
            
            LatticeNode s1 = latticeNodeByDimensions.get(i);
            List<Integer> s1Dimensions = new ArrayList<Integer>(s1.getDimensions());
            int k = s1Dimensions.get(s1Dimensions.size() - 1) + 1;
            for ( ; k < totalContextualDimensions; k++) {
                s1Dimensions.remove(s1Dimensions.size() - 1);
                s1Dimensions.add(k);
                LatticeNode s2 = lit.getLatticeNode(s1Dimensions);
                if (s2 != null) {
                    LatticeNode joined = s1.join(s2, data, denseContextTau, cit);
                    if (joined != null) {
                        if (joined.getDenseContexts().size() != 0) {
                            result.add(joined);
                        }
                    }
                }
                
            }
        }
        
        
        //find out dense candidate subspaces
        /*
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
        */
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
    public List<Datum> contextualOutlierDetection(List<Datum> data, Context context, List<Integer> inputOutlierIndexes) throws Exception {
        //visit the context
        ContextStats.numContextsGenerated++;
        
        //This should be counted as part of lattice building
        long timeTemp1 = System.currentTimeMillis();
        BitSet bs = context.getBitSet();
        List<Integer> indexes = BitSetUtil.bitSet2Indexes(bs);
        long timeTemp2 = System.currentTimeMillis();
        ContextStats.timeBuildLattice += (timeTemp2 - timeTemp1);
        
        long timeDetectContextualOutliersBefore = System.currentTimeMillis();

        if (conf.getTransformType() == TransformType.MAD &&
                (conf.getBoolean(MacroBaseConf.CONTEXTUAL_PRUNING_MAD_NOOUTLIERS, MacroBaseDefaults.CONTEXTUAL_PRUNING_MAD_NOOUTLIERS)||
                        conf.getBoolean(MacroBaseConf.CONTEXTUAL_PRUNING_MAD_CONTAINEDOUTLIERS, MacroBaseDefaults.CONTEXTUAL_PRUNING_MAD_CONTAINEDOUTLIERS))) {
            long time1 = System.currentTimeMillis();
            boolean MADNoOutliersContainedOutliersPruned = MADNoOutliersContainedOutliersPruningTight(context, data, indexes, inputOutlierIndexes);
            long time2 = System.currentTimeMillis();
            ContextStats.timeMADNoOutliersContainedOutliersPruned += (time2 - time1);
            if (MADNoOutliersContainedOutliersPruned) {
                long timeDetectContextualOutliersAfter = System.currentTimeMillis();
                ContextStats.timeDetectContextualOutliers += (timeDetectContextualOutliersAfter - timeDetectContextualOutliersBefore);
                return null;
            }
        }
        
        List<Datum> contextualData = new ArrayList<Datum>();
        for (int i = 0; i < indexes.size(); i++) {
            Datum curDatum = data.get(indexes.get(i));
            contextualData.add(curDatum);
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
        context.setIsZeroMAD(((MAD)((BatchScoreFeatureTransform)featureTransform).getBatchTrainScore()).isZeroMAD());
        context.setInlinerMR();
        
        //is there outliers in this context
        //construct outliers from outlierIndexes
        List<Datum> maximalOutliers = new ArrayList<Datum>();
        if (outlierIndexes.size() > 0) {
            ContextStats.numContextsGeneratedWithOutliers++;
            BitSet outlierBitSet = BitSetUtil.indexes2BitSet(outlierIndexes, data.size());
            context.setOutlierBitSet(outlierBitSet);
            BitSet maximalOutlierBS = numberOutliersNotContainedInAncestor(context);
            if (maximalOutlierBS.cardinality() > 0) {
                context.setNumberOfOutliersNotContainedInAncestor(maximalOutlierBS.cardinality());
                //outliers not contained in parent context outliers
                List<Integer> newOutlierIndexes = BitSetUtil.bitSet2Indexes(maximalOutlierBS);
                for (int i = 0; i < newOutlierIndexes.size(); i++) {
                    maximalOutliers.add(data.get(newOutlierIndexes.get(i)));
                }
                ContextStats.numContextsGeneratedWithMaximalOutliers++;  
            } 
            
        } else {
            ContextStats.numContextsGeneratedWithOutOutliers++;
        }
        
        long timeDetectContextualOutliersAfter = System.currentTimeMillis();
        ContextStats.timeDetectContextualOutliers += (timeDetectContextualOutliersAfter - timeDetectContextualOutliersBefore);
        return maximalOutliers;
    }
    
    private boolean MADNoOutliersContainedOutliersPruningTight(Context context, List<Datum> data, List<Integer> indexes, List<Integer> inputOutlierIndexes) {
        double median = -1;
        double MADLowerBound = -1;
        if (indexes.size() % 2 == 1) {
            int medianIndex = (indexes.size() - 1 )/ 2;
            median = data.get(indexes.get(medianIndex)).getMetrics().getEntry(0);
            
            int step = (indexes.size() - 1) / 4;
            int leftMADIndex = medianIndex - step;
            int rightMADIndex = medianIndex + step;
            double leftMADLowerBound = data.get(indexes.get(leftMADIndex)).getMetrics().getEntry(0);
            double rightMADLowerBound = data.get(indexes.get(rightMADIndex)).getMetrics().getEntry(0);
            MADLowerBound = Math.min(median - leftMADLowerBound, rightMADLowerBound - median);
            
        } else {
            
            median = data.get(indexes.get(indexes.size()/ 2)).getMetrics().getEntry(0)
                    + data.get(indexes.get(indexes.size()/ 2 - 1)).getMetrics().getEntry(0);
            median /= 2;
            
            int step = (indexes.size() / 2 - 1 ) / 2;;
            int leftMADIndex = indexes.size()/ 2 - 1 - step;
            int rightMADIndex = indexes.size()/ 2 + step;
            double leftMADLowerBound = data.get(indexes.get(leftMADIndex)).getMetrics().getEntry(0);
            double rightMADLowerBound = data.get(indexes.get(rightMADIndex)).getMetrics().getEntry(0);
            MADLowerBound = Math.min(median - leftMADLowerBound, rightMADLowerBound - median);
            
        }
        double threshold = conf.getDouble(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, MacroBaseDefaults.OUTLIER_STATIC_THRESHOLD);
        
        double minValue = data.get(indexes.get(0)).getMetrics().getEntry(0);
        double maxValue = data.get(indexes.get(indexes.size() - 1)).getMetrics().getEntry(0);
      
        //simply need to check if the inputOutliers can be outliers
        if (inputOutlierIndexes.size() > 0 && conf.getBoolean(MacroBaseConf.CONTEXTUAL_PRUNING_MAD_NOOUTLIERS, MacroBaseDefaults.CONTEXTUAL_PRUNING_MAD_NOOUTLIERS)) {
            double requiredMADLowerBound = -Double.MIN_VALUE;
            for (int i = 0; i < inputOutlierIndexes.size(); i++) {
                if (context.getInverseBitSet().get(i)) {
                    if (context.getAncestorOutlierBitSet() != null && context.getAncestorOutlierBitSet().get(inputOutlierIndexes.get(i)) )
                        continue;
                    double value = data.get(inputOutlierIndexes.get(i)).getMetrics().getEntry(0);
                    double requiredMADLowerBoundFoCurValue = Math.abs((value - median)) / threshold;
                    if (requiredMADLowerBoundFoCurValue > requiredMADLowerBound) {
                        requiredMADLowerBound = requiredMADLowerBoundFoCurValue;
                    }
                }   
            }
            if (MADLowerBound >= requiredMADLowerBound) {
                ContextStats.numMadNoOutliers++;
                return true;
            } else {
                //check if MAD >= requiredMADLowerBound
                if (MADLowerBoundVerification(context, data, indexes, requiredMADLowerBound)) {
                    ContextStats.numMadNoOutliers++;
                    return true;
                }
            }
            return false;
        } 
        
        if (conf.getBoolean(MacroBaseConf.CONTEXTUAL_PRUNING_MAD_NOOUTLIERS, MacroBaseDefaults.CONTEXTUAL_PRUNING_MAD_NOOUTLIERS)) {
            if (minValue == median && maxValue == median) {
                ContextStats.numMadNoOutliers++; 
                return true;
            }
            
            double requiredMADLowerBoundForMinValue = (median - minValue) / threshold;
            double requiredMADLowerBoundForMaxValue = (maxValue - median) / threshold;
            double requiredMADLowerBound = Math.max(requiredMADLowerBoundForMinValue, requiredMADLowerBoundForMaxValue);
            
            if (MADLowerBound >= requiredMADLowerBound) {
                ContextStats.numMadNoOutliers++;
                return true;
            } else {
                //check if MAD >= requiredMADLowerBound
                if (MADLowerBoundVerification(context, data, indexes, requiredMADLowerBound)) {
                    ContextStats.numMadNoOutliers++;
                    return true;
                }
            }
            
        }
        
        if (context == globalContext)
            return false;
        
        if (conf.getBoolean(MacroBaseConf.CONTEXTUAL_PRUNING_MAD_CONTAINEDOUTLIERS, MacroBaseDefaults.CONTEXTUAL_PRUNING_MAD_CONTAINEDOUTLIERS)) {
            MetricRange ancestorInlinerMR = context.getAncestorInlinerMR();
            //at least one tuple is in the ancestorInlinerMR; otherwise, this context and its ancestors would have been pruned
            //find the smallest value that is in the range, and the largest value that is in the range
            double smallestValueInAncestorInlinerMR = 0;
            double largestValueInAncestorInlinerMR = 0;
            
            //use binary search to find smallestValueInAncestorInlinerMR 
            //find the number of elements < leftBound
            double leftBound = ancestorInlinerMR.getLeft();
            int leftIndex = 0;
            int rightIndex = indexes.size() - 1;
            while (rightIndex >= leftIndex) {
                int middleIndex = (leftIndex + rightIndex) / 2;
                double middleValue = data.get(indexes.get(middleIndex)).getMetrics().getEntry(0);
                if (middleValue < leftBound) {
                    leftIndex = middleIndex + 1;
                } else if (middleValue >= leftBound) {
                    rightIndex = middleIndex - 1;
                }
            }
            if (leftIndex >= 0 && leftIndex < indexes.size()) {
                smallestValueInAncestorInlinerMR = data.get(indexes.get(leftIndex)).getMetrics().getEntry(0);
            } else {
                log.debug("Error: ancestorInlinerMR should contain at least one tuple at this point");
            }
            
            //use binary search to find largestValueInAncestorInlinerMR
            //find number of elements > rightBound
            double rightBound = ancestorInlinerMR.getRight();
            leftIndex = 0;
            rightIndex = indexes.size() - 1;
            while (rightIndex >= leftIndex) {
                int middleIndex = (leftIndex + rightIndex) / 2;
                double middleValue = data.get(indexes.get(middleIndex)).getMetrics().getEntry(0);
                if (middleValue <= rightBound) {
                    leftIndex = middleIndex + 1;
                } else if (middleValue > rightBound) {
                    rightIndex = middleIndex - 1;
                }
            }
            if (rightIndex >= 0 && rightIndex < indexes.size()) {
                largestValueInAncestorInlinerMR = data.get(indexes.get(rightIndex)).getMetrics().getEntry(0);
            } else {
                log.debug("Error: ancestorInlinerMR should contain at least one tuple at this point");
            }
            
            double requiredMADLowerBoundForSmallestValueInAncestorInlinerMR = Math.abs(smallestValueInAncestorInlinerMR - median) / threshold;
            double requiredMADLowerBoundForLargestValueInAncestorInlinerMR = Math.abs(largestValueInAncestorInlinerMR - median) / threshold;
            
            double requiredMADLowerBound = Math.max(requiredMADLowerBoundForSmallestValueInAncestorInlinerMR, requiredMADLowerBoundForLargestValueInAncestorInlinerMR);
            if (MADLowerBound >= requiredMADLowerBound) {
                ContextStats.numMadContainedOutliers++;
                return true;
            } else {
                //check if MAD >= requiredMADLowerBound
                if (MADLowerBoundVerification(context, data, indexes, requiredMADLowerBound)) {
                    ContextStats.numMadContainedOutliers++;
                    return true;
                }
            }
        }
       
        
        return false;
    }
    
    /**
     * Check if the MAD of the indexes has the requiredMADLowerBound
     * @param context
     * @param data
     * @param indexes
     * @param requiredMADLowerBound
     * @return
     */
    private boolean MADLowerBoundVerification(Context context, List<Datum> data, List<Integer> indexes, double requiredMADLowerBound) {
        //count the number of values < requiredMADLowerBound
        int numValuesLessThanRequiredMADLowerBound = 0;
        if (indexes.size() % 2 == 1) {
            int medianIndex = (indexes.size() - 1 )/ 2;
            double median = data.get(indexes.get(medianIndex)).getMetrics().getEntry(0);
            //search the right half 
            int leftIndex = medianIndex;
            int rightIndex = indexes.size() - 1;
            while (rightIndex >= leftIndex) {
                int middleIndex = (leftIndex + rightIndex) / 2;
                double middleValue = data.get(indexes.get(middleIndex)).getMetrics().getEntry(0) - median;
                if (middleValue < requiredMADLowerBound) {
                    leftIndex = middleIndex + 1;
                } else if (middleValue >= requiredMADLowerBound) {
                    rightIndex = middleIndex - 1;
                }
            }
            numValuesLessThanRequiredMADLowerBound += (leftIndex - medianIndex);
            
            //search the left half
            leftIndex = medianIndex - 1;
            rightIndex = 0;
            while (leftIndex >= rightIndex) {
                int middleIndex = (leftIndex + rightIndex) / 2;
                double middleValue = median - data.get(indexes.get(middleIndex)).getMetrics().getEntry(0);
                if (middleValue < requiredMADLowerBound) {
                    leftIndex = middleIndex - 1;
                } else if (middleValue >= requiredMADLowerBound) {
                    rightIndex = middleIndex + 1;
                }
            }
            numValuesLessThanRequiredMADLowerBound += ((medianIndex - 1) - leftIndex) ;
            /*
            int test = 0;
            for (int i = 0; i < indexes.size(); i++) {
                double value = data.get(indexes.get(i)).getMetrics().getEntry(0);
                if (Math.abs(value - median) < requiredMADLowerBound) {
                    test++;
                }
            }
            if (test != numValuesLessThanRequiredMADLowerBound) {
                System.err.println("ERROR");
            }
            */
            if (numValuesLessThanRequiredMADLowerBound <= indexes.size() / 2) {
                return true;
            } else {
                return false;
            }
            
        } else {
            double median = data.get(indexes.get(indexes.size()/ 2)).getMetrics().getEntry(0)
                    + data.get(indexes.get(indexes.size()/ 2 - 1)).getMetrics().getEntry(0);
            median /= 2;
            //search the right half
            int leftIndex = indexes.size()/ 2;
            int rightIndex = indexes.size() - 1;
            while (rightIndex >= leftIndex) {
                int middleIndex = (leftIndex + rightIndex) / 2;
                double middleValue = data.get(indexes.get(middleIndex)).getMetrics().getEntry(0) - median;
                if (middleValue < requiredMADLowerBound) {
                    leftIndex = middleIndex + 1;
                } else if (middleValue >= requiredMADLowerBound) {
                    rightIndex = middleIndex - 1;
                }
            }
            numValuesLessThanRequiredMADLowerBound += (leftIndex - indexes.size()/ 2);
            int finalLeftIndexRightHalf = leftIndex;
            //search the left half
            leftIndex = indexes.size()/ 2 - 1;
            rightIndex = 0;
            while (leftIndex >= rightIndex) {
                int middleIndex = (leftIndex + rightIndex) / 2;
                double middleValue = median - data.get(indexes.get(middleIndex)).getMetrics().getEntry(0);
                if (middleValue < requiredMADLowerBound) {
                    leftIndex = middleIndex - 1;
                } else if (middleValue >= requiredMADLowerBound) {
                    rightIndex = middleIndex + 1;
                }
            }
            numValuesLessThanRequiredMADLowerBound += (indexes.size()/ 2 - 1 - leftIndex) ;
            int finalLeftIndexLeftHalf = leftIndex;
            /*
            int test = 0;
            for (int i = 0; i < indexes.size(); i++) {
                double value = data.get(indexes.get(i)).getMetrics().getEntry(0);
                if (Math.abs(value - median) < requiredMADLowerBound) {
                    test++;
                }
            }
            if (test != numValuesLessThanRequiredMADLowerBound) {
                System.err.println("ERROR");
            }*/
            if (numValuesLessThanRequiredMADLowerBound <= indexes.size() / 2 - 1) {
                return true;
            } else if (numValuesLessThanRequiredMADLowerBound == indexes.size() / 2){
                double middleValue1 = Double.MIN_VALUE;
                if (finalLeftIndexRightHalf-1 >= indexes.size()/ 2) {
                    middleValue1 = Math.max(middleValue1, data.get(indexes.get(finalLeftIndexRightHalf-1)).getMetrics().getEntry(0) - median);
                }
                if (finalLeftIndexLeftHalf+1 <= indexes.size()/ 2 - 1) {
                    middleValue1 = Math.max(middleValue1, median - data.get(indexes.get(finalLeftIndexLeftHalf+1)).getMetrics().getEntry(0));

                }
                double middleValue2 = Double.MAX_VALUE;
                if (finalLeftIndexRightHalf <= indexes.size() - 1) {
                    middleValue2 = Math.min(middleValue2, data.get(indexes.get(finalLeftIndexRightHalf)).getMetrics().getEntry(0) - median);
                }
                if (finalLeftIndexLeftHalf >= 0) {
                    middleValue2 = Math.min(middleValue2,  median - data.get(indexes.get(finalLeftIndexLeftHalf)).getMetrics().getEntry(0));
                }
                double mad = (middleValue1 + middleValue2) /2;
                /*
                double[] tempValues = new double[indexes.size()];
                for (int i = 0; i < indexes.size(); i++) {
                    double value = data.get(indexes.get(i)).getMetrics().getEntry(0);
                    tempValues[i] = Math.abs(value - median);
                }
                Arrays.sort(tempValues);
                double mad = (tempValues[indexes.size() / 2 - 1] + tempValues[indexes.size() / 2])/2;
                if (mad != (middleValue1 + middleValue2) /2) {
                    System.err.println("ERROR");
                }*/
                if (mad >= requiredMADLowerBound) {
                    return true;
                } else {
                    return false;
                } 
            } else  {
                return false;
            }   
        }
    }
    
    
    
    private BitSet numberOutliersNotContainedInAncestor(Context context) {
        BitSet outlierBitSet = context.getOutlierBitSet();
        BitSet ancestorOutlierBitSet = context.getAncestorOutlierBitSet();
        
        if (ancestorOutlierBitSet == null) {
            return outlierBitSet;
        }
        if (outlierBitSet.cardinality() == 0) {
            return outlierBitSet;
         }
         BitSet bsClone = (BitSet)outlierBitSet.clone();
         bsClone.andNot(ancestorOutlierBitSet);
         return bsClone;
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
    public List<LatticeNode> buildOneDimensionalLatticeNodes(List<Datum> data) throws ConfigurationException {
        if (globalContext == null) {
            globalContext = new Context(data, conf);
        }
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

    private List<LatticeNode> buildOneDimensionalLatticeNodesGivenOutliers(List<Datum> data, List<Datum> inputOutliers, List<Integer> inputOutlierIndexes) throws ConfigurationException {
        
        if (this.oneDimensionalLatticeNodes != null) {
            List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
            
            //use previously computed one-dimensional lattice node
            for (LatticeNode oldLatticeNode: oneDimensionalLatticeNodes) {
                LatticeNode ss = new LatticeNode(oldLatticeNode.getDimensions().get(0));
                for (Context denseContext: oldLatticeNode.getDenseContexts()) {
                    
                    Context newDenseContext = new Context(denseContext.getDimensions().get(0), denseContext.getIntervals().get(0), globalContext);
                    newDenseContext.setBitSet(denseContext.getBitSet());
                    boolean containInputOutliers = false;
                    for (int i = 0; i < inputOutliers.size(); i++) {
                        if (newDenseContext.getBitSet().get(inputOutlierIndexes.get(i))) {
                            containInputOutliers = true;
                            break;
                        }
                    }
                    if (containInputOutliers == true) {
                        ss.addDenseContext(newDenseContext);
                        BitSet inverseBitSet = new BitSet(inputOutliers.size());
                        for (int i = 0; i < inputOutliers.size(); i++) {
                            if (newDenseContext.getBitSet().get(inputOutlierIndexes.get(i))) {
                                inverseBitSet.set(i);
                            }
                        }
                        newDenseContext.setInverseBitSet(inverseBitSet);
                    }
                }
                latticeNodes.add(ss);
            }
            
            return latticeNodes;
        }
        
        //create subspaces
        List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
        int discreteDimensions = contextualDiscreteAttributes.size();
        for (int dimension = 0; dimension < totalContextualDimensions; dimension++) {
            LatticeNode ss = new LatticeNode(dimension);
            
            List<Context> denseContexts = null;
            if (dimension < discreteDimensions) {
                //discrete attribute
                denseContexts = initOneDimensionalDenseContextsAndContext2DataGivenOutliers(data, dimension, inputOutliers, inputOutlierIndexes);
            } else {
                denseContexts = initOneDimensionalDenseContextsAndContext2Data(data, dimension, denseContextTau); 
                //remove those denseContexts that does not contain at least one inputOutliers
                List<Context> toBeRemoved = new ArrayList<Context>();
                for (Context context: denseContexts) {
                    boolean containInputOutliers = false;
                    for (int i = 0; i < inputOutliers.size(); i++) {
                        if (context.getBitSet().get(inputOutlierIndexes.get(i))) {
                            containInputOutliers = true;
                            break;
                        }
                    }
                    if (containInputOutliers == false) {
                        toBeRemoved.add(context);
                    }
                }
                denseContexts.removeAll(toBeRemoved);
            }
                
            //set the inverseBitSet
            for (Context context: denseContexts) {
                BitSet inverseBitSet = new BitSet(inputOutliers.size());
                for (int i = 0; i < inputOutliers.size(); i++) {
                    if (context.getBitSet().get(inputOutlierIndexes.get(i))) {
                        inverseBitSet.set(i);
                    }
                }
                context.setInverseBitSet(inverseBitSet);
            }
            
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
            List<Integer> randomSampleIndexes = fixedSampleIndexes(data.size(), 1000);
            double[] sampleValues = new double[randomSampleIndexes.size()];
            for (int i = 0; i < randomSampleIndexes.size(); i++)
                sampleValues[i] = values[randomSampleIndexes.get(i)];
            
          
            HashSet<Interval> allIntervals = new HashSet<Interval>();
            List<Interval> tempIntervals = new Discretization(sampleValues).kMeans(numIntervals);
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

    private List<Context> initOneDimensionalDenseContextsAndContext2DataGivenOutliers(List<Datum> data, int dimension, List<Datum> inputOutliers, List<Integer> inputOutlierIndexes) throws ConfigurationException {
        //consider a context as long as it contains at least one outlier.
        List<Context> contextsContainingOutliers = initOneDimensionalDenseContextsAndContext2Data(inputOutliers, dimension, 1.0 / (inputOutliers.size() + 1));
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

    private List<Integer> randomSampleIndexes(int size, int numSample) {
        List<Integer> sampleDataIndexes = new ArrayList<Integer>();
        Random rnd = new Random();
        for (int i = 0; i < size; i++) {
            if (sampleDataIndexes.size() < numSample) {
                sampleDataIndexes.add(i);
            } else {
                int j = rnd.nextInt(i); //j in [0,i)
                if (j < sampleDataIndexes.size()) {
                    sampleDataIndexes.set(j, i);
                }
            }
        }
        return sampleDataIndexes;
    }

    private List<Integer> fixedSampleIndexes(int size, int numSample) {
        List<Integer> sampleDataIndexes = new ArrayList<Integer>();
        if (numSample >= size) {
            for (int i = 0; i < size; i++) {
                sampleDataIndexes.add(i);
            }
        } else {
            int step = size / numSample;
            for (int i = 0; i < size; i = i + step) {
                sampleDataIndexes.add(i);
            }
        }
        
        return sampleDataIndexes;
    }
}

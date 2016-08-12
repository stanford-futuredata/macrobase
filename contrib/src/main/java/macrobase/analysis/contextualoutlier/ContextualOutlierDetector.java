package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.io.*;

import macrobase.analysis.contextualoutlier.conf.ContextualConf;
import macrobase.analysis.contextualoutlier.conf.ContextualDefaults;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.classify.StaticThresholdClassifier;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
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
    private boolean densityPruning;
    private boolean dependencyPruning;
    private boolean distributionPruningForTraining;
    private boolean distributionPruningForScoring;
    private double alpha = 0.05;
    //This is the outliers detected for every dense context
    private Map<Context, List<OutlierClassificationResult>> context2Outliers = new HashMap<>();

    public ContextualOutlierDetector(MacroBaseConf conf) throws IOException {
        this.conf = conf;
        this.contextualDiscreteAttributes = conf.getStringList(ContextualConf.CONTEXTUAL_DISCRETE_ATTRIBUTES,
                                                               ContextualDefaults.CONTEXTUAL_DISCRETE_ATTRIBUTES);
        this.contextualDoubleAttributes = conf.getStringList(ContextualConf.CONTEXTUAL_DOUBLE_ATTRIBUTES,
                                                             ContextualDefaults.CONTEXTUAL_DOUBLE_ATTRIBUTES);
        this.denseContextTau = conf.getDouble(ContextualConf.CONTEXTUAL_DENSECONTEXTTAU,
                                              ContextualDefaults.CONTEXTUAL_DENSECONTEXTTAU);
        this.numIntervals = conf.getInt(ContextualConf.CONTEXTUAL_NUMINTERVALS,
                                        ContextualDefaults.CONTEXTUAL_NUMINTERVALS);
        this.maxPredicates = conf.getInt(ContextualConf.CONTEXTUAL_MAX_PREDICATES,
                                         ContextualDefaults.CONTEXTUAL_MAX_PREDICATES);
        this.densityPruning = conf.getBoolean(ContextualConf.CONTEXTUAL_PRUNING_DENSITY,
                                              ContextualDefaults.CONTEXTUAL_PRUNING_DENSITY);
        this.dependencyPruning = conf.getBoolean(ContextualConf.CONTEXTUAL_PRUNING_DEPENDENCY,
                                                 ContextualDefaults.CONTEXTUAL_PRUNING_DEPENDENCY);
        this.distributionPruningForTraining = conf.getBoolean(ContextualConf.CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_TRAINING,
                                                              ContextualDefaults.CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_TRAINING);
        this.distributionPruningForScoring = conf.getBoolean(ContextualConf.CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_SCORING,
                                                        ContextualDefaults.CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_SCORING);
        this.totalContextualDimensions = contextualDiscreteAttributes.size() + contextualDoubleAttributes.size();
        this.encoder = conf.getEncoder();
        this.contextualOutputFile = conf.getString(ContextualConf.CONTEXTUAL_OUTPUT_FILE,
                                                   ContextualDefaults.CONTEXTUAL_OUTPUT_FILE);      
        log.debug("There are {} contextualDiscreteAttributes, and {} contextualDoubleAttributes",
                  contextualDiscreteAttributes.size(), contextualDoubleAttributes.size());
    }

    /**
     * Interface 1: search all contextual outliers
     *
     * @param data
     * @throws Exception
     */
    public Map<Context, List<OutlierClassificationResult>> searchContextualOutliers(List<ContextualDatum> data) throws Exception {
        Stopwatch sw = Stopwatch.createUnstarted();
        log.debug("Find global context outliers on data num tuples: {} , MBs {} ", data.size());
        sw.start();
        HashSet<ContextualDatum> sample = randomSampling(data, 100);
        globalContext = new Context(sample, densityPruning, dependencyPruning, alpha);
        contextualOutlierDetection(data, globalContext);
        sw.stop();
        long globalOutlierDetecionTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("Done global context outlier remaining data size {} : (duration: {}ms)", data.size(),
                  globalOutlierDetecionTime);
        List<LatticeNode> preLatticeNodes = new ArrayList<LatticeNode>();
        List<LatticeNode> curLatticeNodes = new ArrayList<LatticeNode>();
        for (int level = 1; level <= totalContextualDimensions; level++) {
            if (level > maxPredicates)
                break;
            log.debug("Build {}-dimensional contexts on all attributes", level);
            sw.start();
            if (level == 1) {
                curLatticeNodes = buildOneDimensionalLatticeNodes(data);
            } else {
                curLatticeNodes = levelUpLattice(preLatticeNodes, data);
            }
            sw.stop();
            long latticeNodesBuildTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
            sw.reset();
            log.debug("Done building {}-dimensional contexts on all attributes (duration: {}ms)", level,
                      latticeNodesBuildTimeCurLevel);
            log.debug("Memory Usage: {}", MemoryUtil.checkMemoryUsage());
            if (curLatticeNodes.size() == 0) {
                log.debug("No more dense contexts, thus no need to level up anymore");
                break;
            }
            log.debug("Find {}-dimensional contextual outliers", level);
            sw.start();
            int numDenseContextsCurLevel = 0;
            //run contextual outlier detection
            for (LatticeNode node : curLatticeNodes) {
                for (Context context : node.getDenseContexts()) {
                    contextualOutlierDetection(data, context);
                    numDenseContextsCurLevel++;
                }
            }
            sw.stop();
            long contextualOutlierDetectionTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
            sw.reset();
            log.debug("Done Find {}-dimensional contextual outliers (duration: {}ms)", level,
                      contextualOutlierDetectionTimeCurLevel);
            log.debug(
                    "Done Find {}-dimensional contextual outliers, there are {} dense contexts(average duration per context: {}ms)",
                    level, numDenseContextsCurLevel,
                    (numDenseContextsCurLevel == 0) ? 0 : contextualOutlierDetectionTimeCurLevel / numDenseContextsCurLevel);
            log.debug("Done Find {}-dimensional contextual outliers, densityPruning2: {}, "
                      + "numOutlierDetectionRunsWithoutTrainingWithoutScoring: {},  "
                      + "numOutlierDetectionRunsWithoutTrainingWithScoring: {},  "
                      + "numOutlierDetectionRunsWithTrainingWithScoring: {}",
                      level, densityPruning2,
                      numOutlierDetectionRunsWithoutTrainingWithoutScoring,
                      numOutlierDetectionRunsWithoutTrainingWithScoring,
                      numOutlierDetectionRunsWithTrainingWithScoring);
            log.debug("----------------------------------------------------------");
            //free up memory
            if (level >= 2) {
                for (LatticeNode node : preLatticeNodes) {
                    for (Context context : node.getDenseContexts()) {
                        context2BitSet.remove(context);
                    }
                }
            }
            preLatticeNodes = curLatticeNodes;
        }
        return context2Outliers;
    }

    private List<ContextualDatum> findInputOutliers(List<ContextualDatum> data) {
        List<ContextualDatum> inputOutliers = new ArrayList<ContextualDatum>();
        if (isEncoderSetup() == false)
            return inputOutliers;
        String contextualAPIOutlierPredicates = conf.getString(ContextualConf.CONTEXTUAL_API_OUTLIER_PREDICATES,
                                                               ContextualDefaults.CONTEXTUAL_API_OUTLIER_PREDICATES);
        String[] splits = contextualAPIOutlierPredicates.split(" = ");
        String columnName = splits[0];
        String columnValue = splits[1];
        int contextualDiscreteAttributeIndex = contextualDiscreteAttributes.indexOf(columnName);
        for (ContextualDatum datum : data) {
            if (contextualDiscreteAttributeIndex != -1) {
                int encodedValue = datum.getContextualDiscreteAttributes().get(contextualDiscreteAttributeIndex);
                if (encoder.getAttribute(encodedValue).getValue().equals(columnValue)) {
                    inputOutliers.add(datum);
                }
            }
        }
        return inputOutliers;
    }

    /**
     * Interface 2: Given some outliers, search contexts for which they are outliers
     *
     * @param data
     * @throws Exception
     */
    public Map<Context, List<OutlierClassificationResult>> searchContextGivenOutliers(List<ContextualDatum> data) throws Exception {
        List<ContextualDatum> inputOutliers = findInputOutliers(data);
        return searchContextGivenOutliers(data, inputOutliers);
    }

    /**
     * Interface 2: Given some outliers, search contexts for which they are outliers
     *
     * @param data
     * @param inputOutliers
     * @throws Exception
     */
    public Map<Context, List<OutlierClassificationResult>> searchContextGivenOutliers(List<ContextualDatum> data, List<ContextualDatum> inputOutliers) throws Exception {
        //result contexts that have the input outliers
        List<Context> result = new ArrayList<Context>();
        if (inputOutliers == null || inputOutliers.size() == 0) {
            log.info("There is no input outliers");
            return context2Outliers;
        }
        Stopwatch sw = Stopwatch.createUnstarted();
        log.debug("Find global context outliers on data num tuples: {} , MBs {} ", data.size());
        sw.start();
        HashSet<ContextualDatum> sample = randomSampling(data, 100);
        globalContext = new Context(sample, densityPruning, dependencyPruning, alpha);
        List<Datum> globalOutliers = contextualOutlierDetection(data, globalContext);
        if (globalOutliers != null && globalOutliers.contains(inputOutliers)) {
            result.add(globalContext);
        }
        sw.stop();
        long globalOutlierDetecionTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("Done global context outlier remaining data size {} : (duration: {}ms)", data.size(),
                  globalOutlierDetecionTime);
        List<LatticeNode> preLatticeNodes = new ArrayList<LatticeNode>();
        List<LatticeNode> curLatticeNodes = new ArrayList<LatticeNode>();
        for (int level = 1; level <= totalContextualDimensions; level++) {
            if (level > maxPredicates)
                break;
            log.debug("Build {}-dimensional contexts on all attributes", level);
            sw.start();
            if (level == 1) {
                curLatticeNodes = buildOneDimensionalLatticeNodesGivenOutliers(data, inputOutliers);
            } else {
                curLatticeNodes = levelUpLattice(preLatticeNodes, data);
            }
            sw.stop();
            long latticeNodesBuildTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
            sw.reset();
            log.debug("Done building {}-dimensional contexts on all attributes (duration: {}ms)", level,
                      latticeNodesBuildTimeCurLevel);
            log.debug("Memory Usage: {}", MemoryUtil.checkMemoryUsage());
            if (curLatticeNodes.size() == 0) {
                log.debug("No more dense contexts, thus no need to level up anymore");
                break;
            }
            log.debug("Find {}-dimensional contextual outliers", level);
            sw.start();
            int numDenseContextsCurLevel = 0;
            //run contextual outlier detection
            for (LatticeNode node : curLatticeNodes) {
                for (Context context : node.getDenseContexts()) {
                    List<Datum> outliers = contextualOutlierDetection(data, context);
                    if (outliers != null && outliers.containsAll(inputOutliers)) {
                        result.add(context);
                    }
                    numDenseContextsCurLevel++;
                }
            }
            sw.stop();
            long contextualOutlierDetectionTimeCurLevel = sw.elapsed(TimeUnit.MILLISECONDS);
            sw.reset();
            log.debug("Done Find {}-dimensional contextual outliers (duration: {}ms)", level,
                      contextualOutlierDetectionTimeCurLevel);
            log.debug(
                    "Done Find {}-dimensional contextual outliers, there are {} dense contexts(average duration per context: {}ms)",
                    level, numDenseContextsCurLevel,
                    (numDenseContextsCurLevel == 0) ? 0 : contextualOutlierDetectionTimeCurLevel / numDenseContextsCurLevel);
            log.debug("Done Find {}-dimensional contextual outliers, densityPruning2: {}, "
                      + "numOutlierDetectionRunsWithoutTrainingWithoutScoring: {},  "
                      + "numOutlierDetectionRunsWithoutTrainingWithScoring: {},  "
                      + "numOutlierDetectionRunsWithTrainingWithScoring: {}",
                      level, densityPruning2,
                      numOutlierDetectionRunsWithoutTrainingWithoutScoring,
                      numOutlierDetectionRunsWithoutTrainingWithScoring,
                      numOutlierDetectionRunsWithTrainingWithScoring);
            log.debug("----------------------------------------------------------");
            //free up memory
            if (level >= 2) {
                for (LatticeNode node : preLatticeNodes) {
                    for (Context context : node.getDenseContexts()) {
                        context2BitSet.remove(context);
                    }
                }
            }
            preLatticeNodes = curLatticeNodes;
        }
        Map<Context, List<OutlierClassificationResult>> context2OutlierClassifierGivenOutlier = new HashMap<>();
        for (Context context : result) {
            context2OutlierClassifierGivenOutlier.put(context, context2Outliers.get(context));
        }
        return context2OutlierClassifierGivenOutlier;
    }

    private HashSet<ContextualDatum> randomSampling(List<ContextualDatum> data, int minSampleSize) {
        List<ContextualDatum> sampleData = new ArrayList<>();
        int numSample = (int) (minSampleSize / denseContextTau);
        Random rnd = new Random();
        for (int i = 0; i < data.size(); i++) {
            ContextualDatum d = data.get(i);
            if (sampleData.size() < numSample) {
                sampleData.add(d);
            } else {
                int j = rnd.nextInt(i); //j in [0,i)
                if (j < sampleData.size()) {
                    sampleData.set(j, d);
                }
            }
        }
        return new HashSet<ContextualDatum>(sampleData);
    }

    /**
     * Walking up the lattice, construct the lattice node, when include those lattice nodes that contain at least one dense context
     *
     * @param latticeNodes
     * @param data
     * @return
     */
    private List<LatticeNode> levelUpLattice(List<LatticeNode> latticeNodes, List<ContextualDatum> data) {
        //sort the subspaces by their dimensions
        Stopwatch sw = Stopwatch.createUnstarted();
        log.debug("\tSorting lattice nodes in level {} by their dimensions ", latticeNodes.get(0).dimensions.size());
        sw.start();
        List<LatticeNode> latticeNodeByDimensions = new ArrayList<>(latticeNodes);
        Collections.sort(latticeNodeByDimensions, new LatticeNode.DimensionComparator());
        sw.stop();
        long sortingTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("\tDone Sorting lattice nodes in level {} by their dimensions (duration: {}ms)",
                  latticeNodes.get(0).dimensions.size(), sortingTime);
        //find out dense candidate subspaces
        List<LatticeNode> result = new ArrayList<LatticeNode>();
        log.debug("\tJoining lattice nodes in level {} by their dimensions ", latticeNodes.get(0).dimensions.size());
        sw.start();
        int numLatticeNodeJoins = 0;
        int numDenseContexts = 0;
        for (int i = 0; i < latticeNodeByDimensions.size(); i++) {
            for (int j = i + 1; j < latticeNodeByDimensions.size(); j++) {
                LatticeNode s1 = latticeNodeByDimensions.get(i);
                LatticeNode s2 = latticeNodeByDimensions.get(j);
                LatticeNode joined = s1.join(s2, data, denseContextTau);
                if (joined != null) {
                    numLatticeNodeJoins++;
                    //only interested in nodes that have dense contexts
                    if (joined.getDenseContexts().size() != 0) {
                        result.add(joined);
                        numDenseContexts += joined.getDenseContexts().size();
                    }
                }
            }
        }
        sw.stop();
        long joiningTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("\tDone Joining lattice nodes in level {} by their dimensions (duration: {}ms)",
                  latticeNodes.get(0).dimensions.size(), joiningTime);
        log.debug("\tDone Joining lattice nodes in level {} by their dimensions,"
                  + " there are {} joins and {} dense contexts (average duration per lattice node pair join: {}ms)",
                  latticeNodes.get(0).dimensions.size(), numLatticeNodeJoins, numDenseContexts,
                  (numLatticeNodeJoins == 0) ? 0 : joiningTime / numLatticeNodeJoins);
        return result;
    }

    private int densityPruning2 = 0;
    private int numOutlierDetectionRunsWithoutTrainingWithoutScoring = 0;
    private int numOutlierDetectionRunsWithoutTrainingWithScoring = 0;
    private int numOutlierDetectionRunsWithTrainingWithScoring = 0;

    /**
     * Run outlier detection algorithm on contextual data
     * The algorithm has to static threhold classifier
     *
     * @param data
     * @param context
     * @return
     * @throws Exception
     */
    public List<Datum> contextualOutlierDetection(List<ContextualDatum> data, Context context) throws Exception {
        BitSet bs = context.getContextualBitSet(data, context2BitSet);
        context2BitSet.put(context, bs);
        List<Datum> contextualData = null;
        Context p1 = (context.getParents().size() > 0) ? context.getParents().get(0) : null;
        Context p2 = (context.getParents().size() > 1) ? context.getParents().get(1) : null;
        boolean requiresTraining = true;
        if (p1 != null && sameDistribution(context, p1)) {
            if (distributionPruningForTraining) {
                context.setDetector(p1.getDetector());
                requiresTraining = false;
            } else {
                context.setDetector(constructDetector());
            }
            if (distributionPruningForScoring) {
                numOutlierDetectionRunsWithoutTrainingWithoutScoring++;
            } else {
                contextualData = new ArrayList<>();
                numOutlierDetectionRunsWithoutTrainingWithScoring++;
            }
        } else if (p2 != null && sameDistribution(context, p2)) {
            if (distributionPruningForTraining) {
                context.setDetector(p2.getDetector());
                requiresTraining = false;
            } else {
                context.setDetector(constructDetector());
            }
            if (distributionPruningForScoring) {
                numOutlierDetectionRunsWithoutTrainingWithoutScoring++;
            } else {
                contextualData = new ArrayList<>();
                numOutlierDetectionRunsWithoutTrainingWithScoring++;
            }
        } else {
            context.setDetector(constructDetector());
            context.setDetector(constructDetector());
            contextualData = new ArrayList<>();
            numOutlierDetectionRunsWithTrainingWithScoring++;
        }
        if (contextualData == null) {
            // pruned by distribution
            return null;
        } else {
            List<Integer> indexes = bitSet2Indexes(bs);
            for (Integer index : indexes) {
                contextualData.add(data.get(index));
            }
            context.setSize(contextualData.size());
            double realDensity = (double) contextualData.size() / data.size();
            if (realDensity < denseContextTau) {
                densityPruning2++;
                return null;
            }
        }
        FeatureTransform featureTransform = new BatchScoreFeatureTransform(context.getDetector(), requiresTraining);

        Map<Long, Datum> idToContextualData = new HashMap<>();
        for(Datum d : contextualData) {
            idToContextualData.put(d.getID(), d);
        }

        featureTransform.consume(contextualData);
        OutlierClassifier outlierClassifier = new StaticThresholdClassifier(conf);
        outlierClassifier.consume(featureTransform.getStream().drain());
        List<Datum> outliers = new ArrayList<>();
        List<OutlierClassificationResult> outlierClassificationResults = outlierClassifier.getStream().drain();
        for (OutlierClassificationResult outlierClassificationResult : outlierClassificationResults) {
            if (outlierClassificationResult.isOutlier()) {
                Datum dAfterTransform = outlierClassificationResult.getDatum();
                outliers.add(idToContextualData.get(dAfterTransform.getParentID()));
            }
        }
        if (outliers.size() > 0) {
            context2Outliers.put(context, outlierClassificationResults);
            if (contextualOutputFile != null) {
                PrintWriter contextualOut = new PrintWriter(new FileWriter(contextualOutputFile,true));
                contextualOut.println("Context: " + context.print(conf.getEncoder()));
                contextualOut.println("\t Number of inliners " + (contextualData.size() - outliers.size()));
                contextualOut.println("\t Number of outliers " + outliers.size());
                contextualOut.close();
            }
        }
        return outliers;
    }

    private boolean sameDistribution(Context p1, Context p2) {
        if (distributionPruningForTraining == false && distributionPruningForScoring == false) {
            return false;
        }
        HashSet<ContextualDatum> sample1 = p1.getSample();
        HashSet<ContextualDatum> sample2 = p2.getSample();
        double[] values1 = new double[sample1.size()];
        int i = 0;
        for (Datum d : sample1) {
            values1[i] = d.metrics().getEntry(0);
            i++;
        }
        double[] values2 = new double[sample2.size()];
        int j = 0;
        for (Datum d : sample2) {
            values2[j] = d.metrics().getEntry(0);
            j++;
        }
        double pValue = TestUtils.kolmogorovSmirnovTest(values1, values2);
        boolean rejectNull = (pValue <= alpha) ? true : false;
        if (rejectNull) {
            //reject null, leads to different distribution
            return false;
        } else {
            //accept null, which is same distribution
            return true;
        }
    }

    /**
     * Every context stores its own detector
     *
     * @return
     * @throws ConfigurationException
     */
    private BatchTrainScore constructDetector() throws ConfigurationException {
        return conf.constructTransform();
    }

    /**
     * Find one dimensional lattice nodes with dense contexts
     *
     * @param data
     * @return
     */
    private List<LatticeNode> buildOneDimensionalLatticeNodes(List<ContextualDatum> data) {
        //create subspaces
        List<LatticeNode> latticeNodes = new ArrayList<LatticeNode>();
        for (int dimension = 0; dimension < totalContextualDimensions; dimension++) {
            LatticeNode ss = new LatticeNode(dimension);
            List<Context> denseContexts = initOneDimensionalDenseContextsAndContext2Data(data, dimension,
                                                                                         denseContextTau);
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

    private List<LatticeNode> buildOneDimensionalLatticeNodesGivenOutliers(List<ContextualDatum> data, List<ContextualDatum> inputOutliers) {
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
     */
    private List<Context> initOneDimensionalDenseContextsAndContext2Data(List<ContextualDatum> data, int dimension, double curDensityThreshold) {
        int discreteDimensions = contextualDiscreteAttributes.size();
        List<Context> result = new ArrayList<Context>();
        if (dimension < discreteDimensions) {
            Map<Integer, List<Integer>> distinctValue2Data = new HashMap<Integer, List<Integer>>();
            for (int i = 0; i < data.size(); i++) {
                ContextualDatum datum = data.get(i);
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
                        BitSet bs = indexes2BitSet(distinctValue2Data.get(value), data.size());
                        context2BitSet.put(context, bs);
                    }
                }
            }
        } else {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            //find out the min, max
            for (ContextualDatum datum : data) {
                double value = datum.getContextualDoubleAttributes().getEntry(dimension - discreteDimensions);
                if (value > max) {
                    max = value;
                }
                if (value < min) {
                    min = value;
                }
            }
            HashSet<Interval> allIntervals = new HashSet<Interval>();
            // divide the interval into numIntervals
            double step = (max - min) / numIntervals;
            double start = min;
            for (int i = 0; i < numIntervals; i++) {
                if (i != numIntervals - 1) {
                    Interval interval = new IntervalDouble(dimension, contextualDoubleAttributes.get(
                            dimension - discreteDimensions), start, start + step);
                    start += step;
                    allIntervals.add(interval);
                } else {
                    //make the max a little bit larger
                    Interval interval = new IntervalDouble(dimension, contextualDoubleAttributes.get(
                            dimension - discreteDimensions), start, max + 0.000001);
                    allIntervals.add(interval);
                }
            }
            //count the interval
            HashMap<Interval, List<Integer>> interval2Data = new HashMap<Interval, List<Integer>>();
            for (int i = 0; i < data.size(); i++) {
                ContextualDatum datum = data.get(i);
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
                        BitSet bs = indexes2BitSet(interval2Data.get(interval), data.size());
                        context2BitSet.put(context, bs);
                    }
                }
            }
        }
        return result;
    }

    private List<Context> initOneDimensionalDenseContextsAndContext2DataGivenOutliers(List<ContextualDatum> data, int dimension, List<ContextualDatum> inputOutliers) {
        List<Context> contextsContainingOutliers = initOneDimensionalDenseContextsAndContext2Data(inputOutliers,
                                                                                                  dimension, 1.0);
        List<Context> result = new ArrayList<Context>();
        //re-initialize context2Bitset
        for (Context context : contextsContainingOutliers) {
            List<Integer> temp = new ArrayList<Integer>();
            for (int i = 0; i < data.size(); i++) {
                ContextualDatum datum = data.get(i);
                if (context.containDatum(datum)) {
                    temp.add(i);
                }
            }
            boolean denseContext = ((double) temp.size() / data.size() >= denseContextTau) ? true : false;
            if (denseContext) {
                BitSet bs = indexes2BitSet(temp, data.size());
                context2BitSet.put(context, bs);
                result.add(context);
            }
        }
        return result;
    }

    //trade memory for efficiency
    private Map<Context, BitSet> context2BitSet = new HashMap<Context, BitSet>();

    private BitSet indexes2BitSet(List<Integer> indexes, int total) {
        BitSet bs = new BitSet(total);
        for (int i = 0; i < indexes.size(); i++) {
            int index = indexes.get(i);
            bs.set(index);
        }
        return bs;
    }

    private List<Integer> bitSet2Indexes(BitSet bs) {
        List<Integer> indexes = new ArrayList<Integer>();
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            // operate on index i here
            indexes.add(i);
            if (i == Integer.MAX_VALUE) {
                break; // or (i+1) would overflow
            }
        }
        return indexes;
    }
}

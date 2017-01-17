package macrobase.analysis.pipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import macrobase.analysis.result.OutlierClassificationResult;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.contextualoutlier.Context;
import macrobase.analysis.contextualoutlier.ContextStats;
import macrobase.analysis.contextualoutlier.ContextualOutlierDetector;
import macrobase.analysis.contextualoutlier.LatticeNode;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.ContextualAnalysisResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.CategoricalMetricTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.util.BitSetUtil;

public class BasicContextualBatchedPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicContextualBatchedPipeline.class);

    List<Datum> originalDatums = new ArrayList<Datum>();
    
    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }
    
    private void metricStats(List<Datum> data) {
        double smallest = Double.MAX_VALUE;
        double largest = Double.MIN_VALUE;
        double total = 0;
        
        for (Datum d: data) {
            double value = d.getMetrics().getEntry(0);
            if (value < smallest) {
                smallest = value;
            }
            if (value > largest) {
                largest = value;
            }
            total += value;
        }
        double average = total / data.size();
        log.info("Done Contextual Outlier Detection Print Stats: SmallestMetric: {}", smallest);
        log.info("Done Contextual Outlier Detection Print Stats: LargestMetric: {}", largest);
        log.info("Done Contextual Outlier Detection Print Stats: AverageMetric: {}", average);

    }

    
    public void inverseExperiment(List<Datum> data) throws Exception {
        int numTrials = 1000;
        int maxNumSuspiciousTuples = 1;
        Map<Integer, List<Integer>> trialNum2RandomIndexes = new HashMap<Integer, List<Integer>>();
        for (int i = 1; i <= numTrials; i++) {
            List<Integer> randomIndexes = randomSampleIndexes(data.size(), maxNumSuspiciousTuples);
            trialNum2RandomIndexes.put(i, randomIndexes);
        }
        
        String filePath = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE);
        PrintWriter out = new PrintWriter(new FileWriter(filePath + "_inverse_experiment.txt"));
        PrintWriter out2 = new PrintWriter(new FileWriter(filePath + "_inverse_varying_num_suspicious_tuples.tsv"));
        out2.println("NumSuspiciousTuples" + "\t" + "AverageTime" + "\t" + "LatticeTime" + "\t" + "DetectTime" + "\t" + "NumGeneratedContexts" + "\t" + "NumMADNoOutlier" + "\t" + "NumContextsWithMaximalOutliers" + "\t" + "TrialTimes");
        
        List<LatticeNode> oneDimensionalNodes = new ContextualOutlierDetector(conf).buildOneDimensionalLatticeNodes(data);
        
        for (int numSuspiciousTuples = 1; numSuspiciousTuples <= maxNumSuspiciousTuples; numSuspiciousTuples++) {
            ContextStats.reset();
            
            List<Long> inverseTimes = inverseExperiment (data, numSuspiciousTuples, numTrials,trialNum2RandomIndexes, oneDimensionalNodes);
            long smallestTime = Long.MAX_VALUE;
            long largestTime = Long.MIN_VALUE;
            double averageTime = 0;
            for (Long inverseTime: inverseTimes) {
                if (inverseTime < smallestTime) {
                    smallestTime = inverseTime;
                }
                if (inverseTime > largestTime) {
                    largestTime = inverseTime;
                }
                averageTime += inverseTime;
            }
            averageTime = averageTime / numTrials;
            out.println("*****************************");
            out.println("Done Inverse Contextual Outlier Detection Print Stats: numSuspiciousTuples " + numSuspiciousTuples);
            out.println("Done Inverse Contextual Outlier Detection Print Stats: smallestTime " + smallestTime);
            out.println("Done Inverse Contextual Outlier Detection Print Stats: largestTime " + largestTime);
            out.println("Done Inverse Contextual Outlier Detection Print Stats: averageTime " + averageTime);

            out.println("Done Inverse Contextual Outlier Detection Print Stats: timeBuildLattice " +  (double)ContextStats.timeBuildLattice / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: timeDetectContextualOutliers " +  (double)ContextStats.timeDetectContextualOutliers / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: timeMADNoOutliersContainedOutliersPruned " +  (double)ContextStats.timeMADNoOutliersContainedOutliersPruned / numTrials);
            
            out.println("Done InverseContextual Outlier Detection Print Stats: numDensityPruning " + (double) ContextStats.numDensityPruning / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numTrivialityPruning " +  (double)ContextStats.numTrivialityPruning / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numContextContainedInOutliersPruning " +  (double)ContextStats.numContextContainedInOutliersPruning / numTrials);
            
            out.println("Done InverseContextual Outlier Detection Print Stats: numMadNoOutliers " + (double) ContextStats.numMadNoOutliers / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numMadContainedOutliers " + (double) ContextStats.numMadContainedOutliers / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numContextsGenerated " +  (double)ContextStats.numContextsGenerated / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numContextsGeneratedWithOutliers " +  (double)ContextStats.numContextsGeneratedWithOutliers / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numContextsGeneratedWithOutOutliers " + (double) ContextStats.numContextsGeneratedWithOutOutliers / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numContextsGeneratedWithMaximalOutliers " +  (double)ContextStats.numContextsGeneratedWithMaximalOutliers / numTrials);

            out.println("Done InverseContextual Outlier Detection Print Stats: numInversePruningNoInputOutliers " +  (double)ContextStats.numInversePruningNoInputOutliers / numTrials);
            out.println("Done InverseContextual Outlier Detection Print Stats: numInversePruningInputOutliersContained " +  (double)ContextStats.numInversePruningInputOutliersContained / numTrials);

            out2.println(numSuspiciousTuples + 
                    "\t" + averageTime + 
                    "\t" + (double)ContextStats.timeBuildLattice / numTrials + 
                    "\t" + (double)ContextStats.timeDetectContextualOutliers / numTrials + 
                    "\t" + (double)ContextStats.numContextsGenerated / numTrials + 
                    "\t" + (double)ContextStats.numMadNoOutliers / numTrials + 
                    "\t" + (double)ContextStats.numContextsGeneratedWithMaximalOutliers / numTrials + 
                    "\t" + inverseTimes.toString());
        }
        out.close();
        out2.close();
    }
    
    private List<Long> inverseExperiment(List<Datum> data, int numSuspiciousTuples, int numTrials, Map<Integer, List<Integer>> trialNum2RandomIndexes, List<LatticeNode> oneDimensionalNodes ) throws Exception {
        String filePath = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE);

        PrintWriter out = new PrintWriter(new FileWriter(filePath + "_inverse_experiment_" + numSuspiciousTuples +"_outliers_trials.tsv"));
        out.println("TrialID" + "\t" + "Time" + "\t" + "LatticeTime" + "\t" + "DetectTime" + "\t" + "NumGeneratedContexts" + "\t" + "NumMADNoOutlier" + "\t" + "NumContextsWithMaximalOutliers");

        
        List<Long> inverseTimes = new ArrayList<Long>();
        for (int i = 1; i <= numTrials; i++) {
            ContextStats.reset();
            
            List<Integer> randomIndexes = trialNum2RandomIndexes.get(i);
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < numSuspiciousTuples; j++) {
                if (j != numSuspiciousTuples) {
                    sb.append(randomIndexes.get(j) + ",");
                } else {
                    sb.append(randomIndexes.get(j));
                }
            }
            conf.set(MacroBaseConf.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX, sb.toString());
           
            List<Datum> inputOutliers = new ArrayList<Datum>();
            String suspiciousTupleIndexes = conf.getString(MacroBaseConf.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX,
                                                                   MacroBaseDefaults.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX);
            if (suspiciousTupleIndexes.equals(MacroBaseDefaults.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX)) {
                //default, all contextual outliers
            } else {
                String[] indexes = suspiciousTupleIndexes.split(",");
                for (String index: indexes) {
                    inputOutliers.add(data.get(Integer.valueOf(index)));
                }
            }
            long timeStart = System.currentTimeMillis();
            ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf, oneDimensionalNodes);
            //invoke different contextual outlier detection APIs
            List<Context> result = contextualDetector.searchContextGivenOutliers(data, inputOutliers);
            System.err.println("numSuspiciousTuples = " + numSuspiciousTuples + " result size = " + result.size());
            for (Context r1: result) {
                System.err.println("\t " + r1.print(conf.getEncoder()));
            }
            long timeEnd = System.currentTimeMillis();
            inverseTimes.add((timeEnd - timeStart) );
            
            out.println(i + 
                    "\t" + (timeEnd - timeStart) + 
                    "\t" + (double)ContextStats.timeBuildLattice  + 
                    "\t" + (double)ContextStats.timeDetectContextualOutliers + 
                    "\t" + (double)ContextStats.numContextsGenerated + 
                    "\t" + (double)ContextStats.numMadNoOutliers  + 
                    "\t" + (double)ContextStats.numContextsGeneratedWithMaximalOutliers
                     );
            out.flush();
        }
        out.close();
        return inverseTimes;
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
    
    public void proxy_multiple_metrics() throws Exception {
        int numMetrics = originalDatums.get(0).getMetrics().getDimension();
        
        Map<Integer, Map<Integer, HashSet<Context>>> metric2TupleID2Contexts = new HashMap<Integer, Map<Integer, HashSet<Context>>>();
        for (int m = 0; m < numMetrics; m++) {
            log.info("Start processing metric: " + m);
            ArrayList<Datum> newData = new ArrayList<Datum>();
            Map<Datum, Datum> new2Original = new HashMap<Datum, Datum>();
            
            for (Datum originalDatum: originalDatums) {
                RealVector newMetric = new ArrayRealVector(1);
                newMetric.setEntry(0, originalDatum.getMetrics().getEntry(m));
                Datum newD = new Datum(originalDatum, newMetric);
                newData.add(newD);
                new2Original.put(newD, originalDatum);
            }
            
            //order the tuples based on metric
            Collections.sort(newData, new Comparator<Datum>(){
                @Override
                public int compare(Datum o1, Datum o2) {
                    if (o1.getMetrics().getEntry(0) > o2.getMetrics().getEntry(0))
                        return 1;
                    else if (o1.getMetrics().getEntry(0) < o2.getMetrics().getEntry(0))
                        return -1;
                    else 
                        return 0;
                }
                
            });
            
            List<Context> contexts = schemaDriven(newData, new ArrayList<Datum>());
            Map<Integer, HashSet<Context>> tupleID2Contexts = new HashMap<Integer,HashSet<Context>>();
            for (Context context: contexts) {
                BitSet maximalOutliersBS = (BitSet) context.getOutlierBitSet().clone();
                if (context.getAncestorOutlierBitSet() != null) {
                    maximalOutliersBS.andNot(context.getAncestorOutlierBitSet());
                }
                List<Integer> indexes = BitSetUtil.bitSet2Indexes(maximalOutliersBS);
                for (Integer index: indexes) {
                    Datum d = newData.get(index);
                    Integer id = originalDatums.indexOf(new2Original.get(d));
                    if (!tupleID2Contexts.containsKey(id)) {
                        HashSet<Context> temp = new HashSet<Context>();
                        tupleID2Contexts.put(id,temp);
                    }
                    tupleID2Contexts.get(id).add(context);
                }    
            }
            metric2TupleID2Contexts.put(m, tupleID2Contexts);
            log.info("End processing metric" + m);
        }
        log.info("Starting collecting tuples with outliers in multiple metrics");
        String filePath = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE);
        PrintWriter out = new PrintWriter(new FileWriter(filePath + "_t_multiple_metrics.txt"));
        for (int i = 0; i < originalDatums.size(); i++) {
            int numFlaggedMetrics = 0;
            for (int m = 0; m < numMetrics; m++){
                if (metric2TupleID2Contexts.containsKey(m) && metric2TupleID2Contexts.get(m).containsKey(i)) {
                    for (Context c: metric2TupleID2Contexts.get(m).get(i)) {
                        double ratio1 = (double)c.getNumberOfOutliersNotContainedInAncestor() / c.getOutlierBitSet().cardinality();
                        double ratio2 = (double)c.getNumberOfOutliersNotContainedInAncestor() / c.getBitSet().cardinality();
                        
                        if (c.getBitSet().cardinality() == originalDatums.size()) 
                            continue;
                        //if (ratio1 > 0.3 && ratio2 < 0.5) {
                            numFlaggedMetrics++;
                            break;
                        //}
                        
                    }
                }
            }
            if (numFlaggedMetrics > 1) {
                for (int m = 0; m < numMetrics; m++){
                    if (metric2TupleID2Contexts.containsKey(m) && metric2TupleID2Contexts.get(m).containsKey(i)) {
                        for (Context c: metric2TupleID2Contexts.get(m).get(i)) {
                            double ratio1 = (double)c.getNumberOfOutliersNotContainedInAncestor() / c.getOutlierBitSet().cardinality();
                            double ratio2 = (double)c.getNumberOfOutliersNotContainedInAncestor() / c.getBitSet().cardinality();
                            if (c.getBitSet().cardinality() == originalDatums.size()) 
                                continue;
                            //if (ratio1 > 0.3 && ratio2 < 0.5) {
                                out.println(i + "\t" +     conf.getStringList(MacroBaseConf.HIGH_METRICS).get(m) + "\t" +
                                        originalDatums.get(i).getMetrics().getEntry(m) + "\t" +
                                        printContextualOutliers(c));
                            //}
                            
                        }
                    }
                }
            }
            out.flush();
        }
        out.close();
        
    }
    
    @Override
    public List<AnalysisResult> run() throws Exception {
        List<AnalysisResult> allARs = new ArrayList<AnalysisResult>();

        //load the data
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();
        
        int numTuples = conf.getInt(MacroBaseConf.CONTEXTUAL_NUM_TUPLES, MacroBaseDefaults.CONTEXTUAL_NUM_TUPLES);
        if (numTuples < data.size())    
            data = data.subList(0, numTuples);
        
        originalDatums = new ArrayList<Datum>(data);
        
        int contextualAlgorithm = conf.getInt(MacroBaseConf.CONTEXTUAL_ALGORITHM, MacroBaseDefaults.CONTEXTUAL_ALGORITHM);
        if (contextualAlgorithm == 3) {
            proxy_multiple_metrics();
        }
        
        List<Datum> inputOutliers = new ArrayList<Datum>();
        String suspiciousTupleIndexes = conf.getString(MacroBaseConf.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX,
                                                               MacroBaseDefaults.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX);
        if (suspiciousTupleIndexes.equals(MacroBaseDefaults.CONTEXTUAL_API_SUSPICIOUS_TUPLES_INDEX)) {
            //default, all contextual outliers
        } else {
            String[] indexes = suspiciousTupleIndexes.split(",");
            for (String index: indexes) {
                inputOutliers.add(data.get(Integer.valueOf(index)));
            }
        }
        
        //If there are categorical metrics, transform them into numerical metrics
        if (!conf.getStringList(MacroBaseConf.CATEGORICAL_METRICS, MacroBaseDefaults.CATEGORICAL_METRICS).isEmpty()
                && conf.getStringList(MacroBaseConf.LOW_METRICS).isEmpty()
                && conf.getStringList(MacroBaseConf.HIGH_METRICS).isEmpty()) {
            CategoricalMetricTransform categoricalMetricTransform = new CategoricalMetricTransform();
            categoricalMetricTransform.consume(data);
            data = categoricalMetricTransform.getStream().drain();
        }
       
        //order the tuples based on metric
        Collections.sort(data, new Comparator<Datum>(){
            @Override
            public int compare(Datum o1, Datum o2) {
                if (o1.getMetrics().getEntry(0) > o2.getMetrics().getEntry(0))
                    return 1;
                else if (o1.getMetrics().getEntry(0) < o2.getMetrics().getEntry(0))
                    return -1;
                else 
                    return 0;
            }
            
        });
       
        
        
        List<Context> schemaDrivenOutliers = null;
        long schemaDrivenDetectionTime = 0;
        if (contextualAlgorithm == 1) {
            //schema driven
            long time1 = System.currentTimeMillis();
            schemaDrivenOutliers = schemaDriven(data,inputOutliers);
            long time2 = System.currentTimeMillis();
            schemaDrivenDetectionTime += (time2 - time1);
            analyzeContextualOutliers(data, schemaDrivenOutliers);
            //topKDiversity(data, schemaDrivenOutliers, 20);
        } else if (contextualAlgorithm == 2) {
            //inverse experiments, spare the loading every time
            inverseExperiment(data);
        }

        metricStats(data);
        log.info("Done Contextual Outlier Detection Print Stats: numTuples: {}", numTuples);
        log.info("Done Contextual Outlier Detection Print Stats: timeTotal: {}", schemaDrivenDetectionTime);
        log.info("Done Contextual Outlier Detection Print Stats: timeBuildLattice: {}", ContextStats.timeBuildLattice);
        log.info("Done Contextual Outlier Detection Print Stats: timeDetectContextualOutliers: {}", ContextStats.timeDetectContextualOutliers);
        log.info("Done Contextual Outlier Detection Print Stats: timeMADNoOutliersContainedOutliersPruned: {}", ContextStats.timeMADNoOutliersContainedOutliersPruned);
        
        log.info("Done Contextual Outlier Detection Print Stats: numDensityPruning: {}", ContextStats.numDensityPruning);
        log.info("Done Contextual Outlier Detection Print Stats: numTrivialityPruning: {}", ContextStats.numTrivialityPruning);
        log.info("Done Contextual Outlier Detection Print Stats: numContextContainedInOutliersPruning: {}", ContextStats.numContextContainedInOutliersPruning);
        
        log.info("Done Contextual Outlier Detection Print Stats: numMadNoOutliers: {}", ContextStats.numMadNoOutliers);
        log.info("Done Contextual Outlier Detection Print Stats: numMadContainedOutliers: {}", ContextStats.numMadContainedOutliers);
        log.info("Done Contextual Outlier Detection Print Stats: numContextsGenerated: {}", ContextStats.numContextsGenerated);
        log.info("Done Contextual Outlier Detection Print Stats: numContextsGeneratedWithOutliers: {}", ContextStats.numContextsGeneratedWithOutliers);
        log.info("Done Contextual Outlier Detection Print Stats: numContextsGeneratedWithOutOutliers: {}", ContextStats.numContextsGeneratedWithOutOutliers);
        log.info("Done Contextual Outlier Detection Print Stats: numContextsGeneratedWithMaximalOutliers: {}", ContextStats.numContextsGeneratedWithMaximalOutliers);
        return allARs;
    }
    
    public List<Context> topKDiversity(List<Datum> data, List<Context> contexts, int k) {
        List<Context> topkResult = new ArrayList<Context>();
        BitSet overallCoverageBS = new BitSet(data.size());
       
        if (contexts.size() <= k) 
            return contexts;
        
        while (topkResult.size() < k) {
            Context bestSelection = null;
            int bestCoverage = 0;
            int bestSize = 0;
            int bestNumPredicates = 0;
            for (Context context: contexts) {
                if (topkResult.contains(context)) {
                    continue;
                }
                BitSet curCoverageBS = (BitSet)context.getOutlierBitSet().clone();
                curCoverageBS.andNot(overallCoverageBS);
                int curCoverage = curCoverageBS.cardinality();
                int curSize = context.getBitSet().cardinality();
                int curNumPredicates = context.getDimensions().size();
                
                if (curCoverage > bestCoverage || 
                        (curCoverage == bestCoverage && curCoverage > 0 && curSize > bestSize) ||
                        (curCoverage == bestCoverage && curCoverage > 0 && curSize == bestSize && curNumPredicates < bestNumPredicates)) {
                    bestSelection = context;
                    bestCoverage = curCoverage;
                    bestSize = curSize;
                    bestNumPredicates = curNumPredicates;
                }
            }
            if (bestSelection == null)
                break;
            overallCoverageBS.or(bestSelection.getOutlierBitSet());
            topkResult.add(bestSelection);
            log.debug("Topk selected context: {}, with outliers {}, increase outliers coverage by {}, overall coverage {}", bestSelection.print(conf.getEncoder()), bestSelection.getOutlierBitSet().cardinality(), bestCoverage, overallCoverageBS.cardinality());

        }
        
        BitSet totalBS = new BitSet(data.size());
        for (Context context: contexts) {
            totalBS.or(context.getOutlierBitSet());
        }
        log.debug("With all outliers in all contexts {}", totalBS.cardinality());
        return topkResult;
    }
    
    public List<Context> schemaDriven(List<Datum> data, List<Datum> inputOutliers) throws Exception {
        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        //invoke different contextual outlier detection APIs
        List<Context> contextWithOutliers = null;
        if (inputOutliers.size() > 0)
            contextWithOutliers = contextualDetector.searchContextGivenOutliers(data, inputOutliers);
        else 
            contextWithOutliers = contextualDetector.searchContextualOutliers(data);

        log.debug("Schema-driven contexts with outliers: {}", contextWithOutliers.size());
        return contextWithOutliers;
    }
   
    
    
    
    public void analyzeContextualOutliers(List<Datum> data, List<Context> contexts) throws Exception {
        String filePath = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE);
        PrintWriter out = new PrintWriter(new FileWriter(filePath));
        for (Context context: contexts) {
            out.println(printContextualOutliers(context));
        }
        out.close();
        
        List<Context> rankedContexts = null;
        
        
        rankedContexts = rankContextsBasedOnOutlierInlierRatio(contexts);
        filePath = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE) + "_ranked_outlier_inlier_ratio.txt";
        out = new PrintWriter(new FileWriter(filePath));
        for (Context context: rankedContexts) {
            out.println(printContextualOutliers(context));
        }
        out.close();
        
        rankedContexts = rankContextsBasedOnNumMaximalContextualOutliers(contexts);
        filePath = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE) + "_ranked_number_maximal_outliers.txt";
        out = new PrintWriter(new FileWriter(filePath));
        for (Context context: rankedContexts) {
            out.println(printContextualOutliers(context));
        }
        out.close();
        
        rankedContexts = rankContextsBasedOnMaximalOutliersToOutliersRatio(contexts);
        filePath = conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE) + "_ranked_maximaloutlier_outlier_ratio.txt";
        out = new PrintWriter(new FileWriter(filePath));
        for (Context context: rankedContexts) {
            out.println(printContextualOutliers(context));
        }
        out.close();
      
    }
    
    private String printContextualOutliers(Context c) {
        StringBuilder sb = new StringBuilder();
        sb.append( c.print(conf.getEncoder()) );
        sb.append("\t");
        sb.append("Inliner Range: " + c.getInlinerMR().toString());
        //sb.append("\t");
        //sb.append("Ancestor Inliner Range: " + c.getAncestorInlinerMR().toString());
        sb.append("\t");
        sb.append("Context Size: " + c.getSize());
        sb.append("\t");
        sb.append("Number of Outliers: " + c.getOutlierBitSet().cardinality());
        sb.append("\t");
        sb.append("Number of Maximal Contextual Outliers: " + c.getNumberOfOutliersNotContainedInAncestor());
        sb.append("\t");
        sb.append("Ratio of Maximal Contextual Outliers over Inliers: " + (double)c.getNumberOfOutliersNotContainedInAncestor() / (c.getSize() - c.getNumberOfOutliersNotContainedInAncestor()));
        sb.append("\t");
        sb.append("Ratio of Maximal Contextual Outliers over Outliers: " + (double)c.getNumberOfOutliersNotContainedInAncestor() / c.getOutlierBitSet().cardinality());

        return sb.toString();
    }
    
   
    /**
     * Rank the contexts based on the level in the lattice, i.e., the number of predicates in the context. 
     * The smaller, the better
     * @param context2Outliers
     * @return
     * @throws Exception
     */
    private List<Context> rankContextsBasedOnLatticeLevel(List<Context> contexts) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>(contexts);
        Collections.sort(rankedContexts, new Comparator<Context>(){
            @Override
            public int compare(Context o1, Context o2) {
                if (o1.getIntervals().size() > o2.getIntervals().size()) 
                    return 1;
                else if (o1.getIntervals().size() < o2.getIntervals().size()) 
                    return -1;
                else 
                    return 0;
            }
            
        });
        return rankedContexts;
    }
    
    /**
     * Rank the contexts based on the size of the context, i.e., the number of tuples
     * The bigger, the better
     * @param context2Outliers
     * @return
     * @throws Exception
     */
    private List<Context> rankContextsBasedOnContextSize(List<Context> contexts) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>(contexts);
        Collections.sort(rankedContexts, new Comparator<Context>(){
            @Override
            public int compare(Context o1, Context o2) {
                if (o1.getSize() > o2.getSize()) 
                    return -1;
                else if (o1.getSize() < o2.getSize()) 
                    return 1;
                else 
                    return 0;
            }
            
        });
        return rankedContexts;
    }
    
    
    /**
     * Rank the contexts based on the outliers/inliners in the context
     * The smaller, the better
     * @param context2Outliers
     * @return
     * @throws Exception
     */
    private List<Context> rankContextsBasedOnMaximalOutliersToOutliersRatio(List<Context> contexts) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>();
        
        Map<Context, Double> context2Ratio = new HashMap<Context, Double>();
        for (Context context : contexts) {
            int numMaximalContextualOutliers = context.getNumberOfOutliersNotContainedInAncestor();
            int numOutliers = context.getOutlierBitSet().cardinality();
            double ratio = (double)numMaximalContextualOutliers/ numOutliers;
            context2Ratio.put(context, ratio);
        }
        rankedContexts.addAll(contexts);
        Collections.sort(rankedContexts,new Comparator<Context>(){
            @Override
            public int compare(Context o1, Context o2) {
                if (context2Ratio.get(o1) > context2Ratio.get(o2)) {
                    return -1;
                } else if (context2Ratio.get(o1) < context2Ratio.get(o2)) {
                    return 1;
                } else {
                    return 0;
                }
            }
            
        });
        return rankedContexts;
    }
   
    
    /**
     * Rank the contexts based on the outliers/inliners in the context
     * The smaller, the better
     * @param context2Outliers
     * @return
     * @throws Exception
     */
    private List<Context> rankContextsBasedOnOutlierInlierRatio(List<Context> contexts) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>();
        
        Map<Context, Double> context2OutlierInlierRatio = new HashMap<Context, Double>();
        for (Context context : contexts) {
            int numMaximalContextualOutliers = context.getNumberOfOutliersNotContainedInAncestor();
            int contextSize = context.getSize();
            double ratio = (double)numMaximalContextualOutliers/ contextSize;
            context2OutlierInlierRatio.put(context, ratio);
        }
        rankedContexts.addAll(contexts);
        Collections.sort(rankedContexts,new Comparator<Context>(){
            @Override
            public int compare(Context o1, Context o2) {
                if (context2OutlierInlierRatio.get(o1) > context2OutlierInlierRatio.get(o2)) {
                    return -1;
                } else if (context2OutlierInlierRatio.get(o1) < context2OutlierInlierRatio.get(o2)) {
                    return 1;
                } else {
                    return 0;
                }
            }
            
        });
        return rankedContexts;
    }
   
    /**
     * Rank the contexts based on the outliers/inliners in the context
     * The smaller, the better
     * @param context2Outliers
     * @return
     * @throws Exception
     */
    private List<Context> rankContextsBasedOnNumMaximalContextualOutliers(List<Context> contexts) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>();
        
        rankedContexts.addAll(contexts);
        Collections.sort(rankedContexts,new Comparator<Context>(){
            @Override
            public int compare(Context o1, Context o2) {
                if (o1.getNumberOfOutliersNotContainedInAncestor() > o2.getNumberOfOutliersNotContainedInAncestor()) {
                    return -1;
                } else if (o1.getNumberOfOutliersNotContainedInAncestor() < o2.getNumberOfOutliersNotContainedInAncestor()) {
                    return 1;
                } else {
                    return 0;
                }
            }
            
        });
        return rankedContexts;
    }
    
  }

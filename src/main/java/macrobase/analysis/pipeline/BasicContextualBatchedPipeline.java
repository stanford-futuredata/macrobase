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

import macrobase.analysis.result.OutlierClassificationResult;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.contextualoutlier.Context;
import macrobase.analysis.contextualoutlier.ContextualOutlierDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.ContextualAnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.conf.MacroBaseConf.ContextualAPI;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;

public class BasicContextualBatchedPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicContextualBatchedPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        List<AnalysisResult> allARs = new ArrayList<AnalysisResult>();
        long time1 = System.currentTimeMillis();
        //load the data
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();
        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        Map<Context, List<OutlierClassificationResult>> context2Outliers = null;
        long time2 = System.currentTimeMillis();
        
        //invoke different contextual outlier detection APIs
        if (contextualAPI == ContextualAPI.findAllContextualOutliers) {
            context2Outliers = contextualDetector.searchContextualOutliers(data);
        } else if (contextualAPI == ContextualAPI.findContextsGivenOutlierPredicate) {
            context2Outliers = contextualDetector.searchContextGivenOutliers(data);
        }
        long time3 = System.currentTimeMillis();
        long loadMs = time2 - time1;
        long executeMs = time3 - time2;
        
        analyzeContextualOutliers(data, context2Outliers);
        
        //summarize every contextual outliers found
        /*
        for (Context context : context2Outliers.keySet()) {
            log.info("Context: " + context.print(conf.getEncoder()));
            BatchSummarizer summarizer = new BatchSummarizer(conf);
            summarizer.consume(context2Outliers.get(context));
            Summary result = summarizer.getStream().drain().get(0);
            long summarizeMs = result.getCreationTimeMs();
            ContextualAnalysisResult ar = new ContextualAnalysisResult(context, result.getNumOutliers(),
                                                                       result.getNumInliers(),
                                                                       loadMs,
                                                                       executeMs,
                                                                       summarizeMs,
                                                                       result.getItemsets());
            allARs.add(ar);
        }*/
        return allARs;
    }
    
    public void analyzeContextualOutliers(List<Datum> data, Map<Context, List<OutlierClassificationResult>> context2Outliers) throws Exception {
        initializeContext2OutlierInlierDatums(data, context2Outliers);
        
        List<Context> rankedContexts = null;
        
        //rank on outlier inlier ratio
        rankedContexts = rankContextsBasedOnOutlierInlierRatio(context2Outliers);
        writeContextualOutliersToFile(rankedContexts, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE)+"_rankedBasedOnOutlierInlierRatio");

        writeContextMetricsToFile(rankedContexts, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE));
       
        
        
        rankedContexts = rankContextsBasedOnLatticeLevel(context2Outliers);
        writeContextualOutliersToFile(rankedContexts, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE)+"_rankedBasedOnLatticeLevel");

        rankedContexts = rankContextsBasedOnContextSize(context2Outliers);
        writeContextualOutliersToFile(rankedContexts, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE)+"_rankedBasedOnContextSize");
        /*
        rankedContexts = rankContextsBasedOnDiversity(context2Outliers, data, new ArrayList<Context>(context2Outliers.keySet()), 0.9);
        writeContextualOutliersToFile(rankedContexts, context2Outliers, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE)+"_rankedBasedOnDiversityDot9");
        
        rankedContexts = rankContextsBasedOnDiversity(context2Outliers, data, new ArrayList<Context>(context2Outliers.keySet()), 0.7);
        writeContextualOutliersToFile(rankedContexts, context2Outliers, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE)+"_rankedBasedOnDiversityDot7");
        
        rankedContexts = rankContextsBasedOnDiversity(context2Outliers, data, new ArrayList<Context>(context2Outliers.keySet()), 0.5);
        writeContextualOutliersToFile(rankedContexts, context2Outliers, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE)+"_rankedBasedOnDiversityDot5");
        */
        
        
        List<Context> filteredRankedContexts = fitlerContextsBasedOnOutlierInlierRatio(context2Outliers);
        writeContextualOutliersToFile(filteredRankedContexts, conf.getString(MacroBaseConf.CONTEXTUAL_OUTPUT_FILE,MacroBaseDefaults.CONTEXTUAL_OUTPUT_FILE)+"_filteredRankedBasedOnOutlierInlierRatio");

    }
    
    
    private Map<Context, HashSet<Datum>> context2OutlierDatums = new HashMap<Context, HashSet<Datum>>();
    private Map<Context, HashSet<Datum>> context2InlinerDatums = new HashMap<Context, HashSet<Datum>>();
    
    private void initializeContext2OutlierInlierDatums(List<Datum> data, Map<Context, List<OutlierClassificationResult>> context2Outliers) {
        Map<Long, Datum> datumID2Datum = new HashMap<Long, Datum>();
        for (Datum datum: data) {
            datumID2Datum.put(datum.getID(), datum);
        }
        
        for (Context context: context2Outliers.keySet()) {
            HashSet<Datum> outlierDatums = new HashSet<Datum>();
            HashSet<Datum> inlinerDatums = new HashSet<Datum>();
            for (OutlierClassificationResult r: context2Outliers.get(context)) {
                if (r.isOutlier()) {
                    Datum outlierDatum = datumID2Datum.get(r.getDatum().getParentID());
                    outlierDatums.add(outlierDatum);
                } else {
                    Datum inlinerDatum = datumID2Datum.get(r.getDatum().getParentID());
                    inlinerDatums.add(inlinerDatum);
                }
            }
            context2OutlierDatums.put(context, outlierDatums);
            context2InlinerDatums.put(context, inlinerDatums);
        }
    }
    
    
    private void writeContextualOutliersToFile(List<Context> rankedContexts, String fileName) throws Exception {
        File file = new File(fileName);
        PrintWriter contextualOut = new PrintWriter(new FileWriter(file));
        for (Context context: rankedContexts) {
            HashSet<Datum> outlierDatums = context2OutlierDatums.get(context);
            HashSet<Datum> inlinerDatums = context2InlinerDatums.get(context);
            contextualOut.println("Context: " + context.print(conf.getEncoder()) + "\tNumOutliers: " + outlierDatums.size() + "\tNumInliers: " + inlinerDatums.size());   
        }
        contextualOut.close();
    }
    
    private void writeContextMetricsToFile(List<Context> rankedContexts, String fileName) throws Exception {
        int index = 1;
        for (Context context: rankedContexts) {
            HashSet<Datum> outlierDatums = context2OutlierDatums.get(context);
            HashSet<Datum> inlinerDatums = context2InlinerDatums.get(context);
            
            //write the context to a specific file
            String contextFileName = fileName + "_" + index + ".txt";
            PrintWriter contextFileNameOut = new PrintWriter(new FileWriter(contextFileName));
            contextFileNameOut.println(context.print(conf.getEncoder()));
            contextFileNameOut.close();
            
            PrintWriter contextFileNameOutliersOut = new PrintWriter(new FileWriter(contextFileName + "_outliers.txt"));
            for (Datum outlierDatum: outlierDatums) {
                contextFileNameOutliersOut.println(outlierDatum.getMetrics().getEntry(0));
            }
            contextFileNameOutliersOut.close();
            PrintWriter contextFileNameInlinersOut = new PrintWriter(new FileWriter(contextFileName + "_inliners.txt"));
            for (Datum inlinerDatum: inlinerDatums) {
                contextFileNameInlinersOut.println(inlinerDatum.getMetrics().getEntry(0));
            }
            contextFileNameInlinersOut.close();
            index++;
        }
    }
    
    /**
     * Rank the contexts based on the level in the lattice, i.e., the number of predicates in the context. 
     * The smaller, the better
     * @param context2Outliers
     * @return
     * @throws Exception
     */
    private List<Context> rankContextsBasedOnLatticeLevel(Map<Context, List<OutlierClassificationResult>> context2Outliers) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>(context2Outliers.keySet());
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
    private List<Context> rankContextsBasedOnContextSize(Map<Context, List<OutlierClassificationResult>> context2Outliers) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>(context2Outliers.keySet());
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
    private List<Context> rankContextsBasedOnOutlierInlierRatio(Map<Context, List<OutlierClassificationResult>> context2Outliers) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>();
        
        Map<Context, Double> context2OutlierInlierRatio = new HashMap<Context, Double>();
        for (Context context : context2Outliers.keySet()) {
            HashSet<Datum> outlierDatums = context2OutlierDatums.get(context);
            HashSet<Datum> inlinerDatums = context2InlinerDatums.get(context);
            double outlierInlierRatio = (double) outlierDatums.size() / inlinerDatums.size();
            context2OutlierInlierRatio.put(context, outlierInlierRatio);
        }
        rankedContexts.addAll(context2Outliers.keySet());
        Collections.sort(rankedContexts,new Comparator<Context>(){
            @Override
            public int compare(Context o1, Context o2) {
                if (context2OutlierInlierRatio.get(o1) > context2OutlierInlierRatio.get(o2)) {
                    return 1;
                } else if (context2OutlierInlierRatio.get(o1) < context2OutlierInlierRatio.get(o2)) {
                    return -1;
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
    private List<Context> fitlerContextsBasedOnOutlierInlierRatio(Map<Context, List<OutlierClassificationResult>> context2Outliers) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>();
        
        Map<Context, Double> context2OutlierInlierRatio = new HashMap<Context, Double>();
        for (Context context : context2Outliers.keySet()) {
            HashSet<Datum> outlierDatums = context2OutlierDatums.get(context);
            HashSet<Datum> inlinerDatums = context2InlinerDatums.get(context);
            double outlierInlierRatio = (double) outlierDatums.size() / inlinerDatums.size();
            context2OutlierInlierRatio.put(context, outlierInlierRatio);
        }
        rankedContexts.addAll(context2Outliers.keySet());
        Collections.sort(rankedContexts,new Comparator<Context>(){
            @Override
            public int compare(Context o1, Context o2) {
                if (context2OutlierInlierRatio.get(o1) > context2OutlierInlierRatio.get(o2)) {
                    return 1;
                } else if (context2OutlierInlierRatio.get(o1) < context2OutlierInlierRatio.get(o2)) {
                    return -1;
                } else {
                    return 0;
                }
            }
            
        });
        
        int i = 0;
        for (i = 0; i < rankedContexts.size(); i++) {
            if (context2OutlierInlierRatio.get(rankedContexts.get(i)) > 0.1) {
                break;
            }
        }
        return rankedContexts.subList(0, i);
    }
    
    
    
    /**
     * Rank the context based on diversity, the top contexts's similarity must be less than similarityThreshold
     * @param context2Outliers
     * @param data
     * @param contexts
     * @param similarityThreshold
     * @return
     * @throws Exception
     */
    private List<Context> rankContextsBasedOnDiversity(Map<Context, List<OutlierClassificationResult>> context2Outliers, List<Datum> data, List<Context> contexts, double similarityThreshold) throws Exception {
        List<Context> rankedContexts = new ArrayList<Context>();
        
    
        Integer[][] contextualOutliers = new Integer[data.size()][contexts.size()];
        for (int i = 0; i < data.size(); i++) {
            for (int j = 0; j < contexts.size(); j++) {
                if (context2OutlierDatums.get(contexts.get(j)).contains(data.get(i))){
                    //this datum is an outlier
                    contextualOutliers[i][j] = 1;
                } else if (context2InlinerDatums.get(contexts.get(j)).contains(data.get(i))){
                    //this datum is an inliner
                    contextualOutliers[i][j] = -1;
                } else {
                    //this datum does not belong to this context
                    contextualOutliers[i][j] = 0;
                }
            }
        }

        //initialize context2OutlierDatumBitSet and context2InlierDatumBitSet
        Map<Context, BitSet> context2OutlierDatumBitSet = new HashMap<Context, BitSet>();
        Map<Context, BitSet> context2InlierDatumBitSet = new HashMap<Context, BitSet>();
        for (int j = 0; j < contexts.size(); j++) {
            BitSet bsOutlier = new BitSet(data.size());
            BitSet bsInlier = new BitSet(data.size());
            for (int i = 0; i < data.size(); i++) {
                if (contextualOutliers[i][j] == 1) {
                    bsOutlier.set(i);
                } else if(contextualOutliers[i][j] == -1) {
                    bsInlier.set(i);
                }
            }
            context2OutlierDatumBitSet.put(contexts.get(j), bsOutlier);
            context2InlierDatumBitSet.put(contexts.get(j), bsInlier);
        }
        
        //analyze the overlap between contexts
        Map<Pair<Context,Context>, Double> contextPair2OutlierOverlap = new HashMap<Pair<Context,Context>, Double>();
        Map<Pair<Context,Context>, Double> contextPair2InlierOverlap = new HashMap<Pair<Context,Context>, Double>();
        Map<Pair<Context,Context>, Double> contextPair2ContextOverlap = new HashMap<Pair<Context,Context>, Double>();

        for (int i = 0; i < contexts.size(); i++) {
            for(int j = i + 1; j < contexts.size(); j++) {
                Context ci = contexts.get(i);
                Context cj = contexts.get(j);
                //overlap between outliers
                BitSet biOutlier = context2OutlierDatumBitSet.get(ci);
                BitSet bjOutlier = context2OutlierDatumBitSet.get(cj);
                BitSet bsANDOutlier = (BitSet) biOutlier.clone();
                bsANDOutlier.and(bjOutlier);
                BitSet bsOROutlier = (BitSet) biOutlier.clone();
                bsOROutlier.or(bjOutlier);
                double overlapOutlier = (double) bsANDOutlier.cardinality() / bsOROutlier.cardinality();
                contextPair2OutlierOverlap.put(Pair.of(ci, cj), overlapOutlier);
                //overlap between inliers
                BitSet biInlier = context2InlierDatumBitSet.get(ci);
                BitSet bjInlier = context2InlierDatumBitSet.get(cj);
                BitSet bsANDInlier = (BitSet) biInlier.clone();
                bsANDInlier.and(bjInlier);
                BitSet bsORInlier = (BitSet) biInlier.clone();
                bsORInlier.or(bjInlier);
                double overlapInlier = (double) bsANDInlier.cardinality() / bsORInlier.cardinality();
                contextPair2InlierOverlap.put(Pair.of(ci, cj), overlapInlier);
                //overlap between contexts, including outliers and inliners
                BitSet biContext = (BitSet) biOutlier.clone();
                biContext.or(biInlier);
                BitSet bjContext = (BitSet) bjOutlier.clone();
                bjContext.or(bjInlier);
                BitSet bsANDContext = (BitSet) biContext.clone();
                bsANDContext.and(bjContext);;
                BitSet bsORContext = (BitSet)biContext.clone();
                bsORContext.or(bjContext);
                double overlapContext = (double) bsANDContext.cardinality() / bsORContext.cardinality();
                contextPair2ContextOverlap.put(Pair.of(ci, cj), overlapContext);
            }
        }
        
        //now rank the contexts in a greedy manner
        HashSet<Context> remainingContexts = new HashSet<Context>(contexts);
        while (remainingContexts.size() > 0) {
            //pick a context that diversifies the most
            Context pick = null;
            if (rankedContexts.size() == 0) {
               pick = contexts.get(0);
            } else {
                double minDiversityScore = Double.MIN_VALUE;
                for (Context curContext: remainingContexts) {
                    double diversityScore = 0;
                    for (int j = 0; j < rankedContexts.size(); j++) {
                        Context c = rankedContexts.get(j);
                        double oneDiversityScore = Double.MIN_VALUE;
                        if (contextPair2OutlierOverlap.containsKey(Pair.of(curContext, c))) {
                            oneDiversityScore = 1.0 - contextPair2OutlierOverlap.get(Pair.of(curContext, c));
                        } else if (contextPair2OutlierOverlap.containsKey(Pair.of(c, curContext))) {
                            oneDiversityScore = 1.0 - contextPair2OutlierOverlap.get(Pair.of(c, curContext));
                        } else {
                            throw new Exception("contextPair2OutlierOverlap does not contain the context pair");
                        }
                        diversityScore += oneDiversityScore;
                    }
                    if(diversityScore > minDiversityScore) {
                        pick = curContext;
                        minDiversityScore = diversityScore;
                    }
                }
            }
            //add the pick
            rankedContexts.add(pick);
            remainingContexts.remove(pick);
            //remove from remainingContexst the ones that are too similar to pick
            HashSet<Context> similarToPick = new HashSet<Context>();
            for (Context c: remainingContexts) {
                double similarity = contextPair2OutlierOverlap.containsKey(Pair.of(pick, c))?contextPair2OutlierOverlap.get(Pair.of(pick, c)):contextPair2OutlierOverlap.get(Pair.of(c, pick));
                if (similarity > similarityThreshold) {
                    similarToPick.add(c);
                }
            }
            remainingContexts.removeAll(similarToPick);
        }
        
        return rankedContexts;
    }
}

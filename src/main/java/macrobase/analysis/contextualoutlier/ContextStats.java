package macrobase.analysis.contextualoutlier;

import java.util.HashMap;
import java.util.Map;

public class ContextStats {

    //density pruning should use now all tuples
    //public static int numDensityPruningsUsingSample = 0;
    public static int numDensityPruning = 0;    
    public static int numTrivialityPruning = 0;
    public static int numContextContainedInOutliersPruning = 0;
    
    public static int numContextsGenerated = 0;
    public static int numContextsGeneratedWithOutliers = 0;
    public static int numContextsGeneratedWithOutOutliers = 0;
    public static int numContextsGeneratedWithMaximalOutliers = 0;
    
    public static int numMadNoOutliers = 0;
    public static int numMadContainedOutliers = 0;
    public static int numMadSubsumption = 0;
    
    public static long timeBuildLattice = 0;
    public static long timeDetectContextualOutliers = 0;
    public static long timeMADNoOutliersContainedOutliersPruned = 0;
    
    
    public static long numInversePruningNoInputOutliers = 0;
    public static long numInversePruningInputOutliersContained = 0;
    
    public static void reset() {
        ContextStats.numDensityPruning = 0;
        ContextStats.numTrivialityPruning = 0;
        ContextStats.numContextContainedInOutliersPruning = 0;
        
        ContextStats.numContextsGenerated = 0;
        ContextStats.numContextsGeneratedWithOutliers = 0;
        ContextStats.numContextsGeneratedWithOutOutliers = 0;
        ContextStats.numContextsGeneratedWithMaximalOutliers = 0;
        
        ContextStats.numMadNoOutliers = 0;
        ContextStats.numMadContainedOutliers = 0;
        ContextStats.numMadSubsumption = 0;
        
        ContextStats.timeBuildLattice = 0;
        ContextStats.timeDetectContextualOutliers = 0;
        ContextStats.timeMADNoOutliersContainedOutliersPruned = 0;
        
        ContextStats.numInversePruningNoInputOutliers = 0;
        ContextStats.numInversePruningInputOutliersContained = 0;
    }
    
}

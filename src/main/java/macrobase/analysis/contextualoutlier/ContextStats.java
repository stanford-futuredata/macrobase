package macrobase.analysis.contextualoutlier;

import java.util.HashMap;
import java.util.Map;

public class ContextStats {

    //density pruning should use now all tuples
    //public static int numDensityPruningsUsingSample = 0;
    public static int numDensityPruningsUsingAll = 0;    
    public static int numTrivialityPruning = 0;
    public static int numSubsumptionPruning = 0;
    
    
    public static int numContextsGenerated = 0;
    public static int numContextsGeneratedWithOutliers = 0;
    public static int numContextsGeneratedWithOutOutliers = 0;
    public static int numContextsGeneratedWithMaximalOutliers = 0;

    
    public static int numDataDrivenContextsPruning = 0;
    
    public static long timeBuildLattice = 0;
    public static long timeDetectContextualOutliers = 0;
    
}

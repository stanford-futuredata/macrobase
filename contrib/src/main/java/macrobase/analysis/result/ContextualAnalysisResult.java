package macrobase.analysis.result;

import java.util.List;

import macrobase.analysis.contextualoutlier.Context;
import macrobase.analysis.summary.itemset.result.ItemsetResult;

public class ContextualAnalysisResult extends AnalysisResult{

    private Context context;
    
    public ContextualAnalysisResult(Context context, double numOutliers,
            double numInliers,
            long loadTime,
            long executionTime,
            long summarizationTime,
            List<ItemsetResult> itemSets) {
        super(numOutliers, numInliers, loadTime, executionTime, summarizationTime, itemSets);
        this.context = context;
    }
    
    public Context getContext() {
        return context;
    }
    
    @Override
    public String toString() {
        String contextString = context.toString();
        return contextString + "\n" + super.toString();
    }
    
}

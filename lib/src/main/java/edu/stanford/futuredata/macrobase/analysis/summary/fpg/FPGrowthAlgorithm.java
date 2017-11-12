package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;

import java.util.List;
import java.util.Set;

public interface FPGrowthAlgorithm {
    void setCombinationsEnabled(boolean flag);

    List<FPGItemsetResult> getEmergingItemsetsWithMinSupport(
            List<Set<Integer>> inliers,
            List<Set<Integer>> outliers,
            double minSupport,
            double minRatio
    );
}

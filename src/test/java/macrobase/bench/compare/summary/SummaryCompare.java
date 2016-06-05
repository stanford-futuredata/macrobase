package macrobase.bench.compare.summary;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.datamodel.Datum;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SummaryCompare {
    public abstract Map<Set<Integer>, Integer> compare(List<Datum> inliers,
                                                       List<Datum> outliers);
}

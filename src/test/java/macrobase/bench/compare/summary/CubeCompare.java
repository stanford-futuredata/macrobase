package macrobase.bench.compare.summary;


import com.google.common.collect.Sets;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CubeCompare {
    private static final Logger log = LoggerFactory.getLogger(CubeCompare.class);

    Map<Set<Integer>, Integer> cnt = new HashMap<>();

    public Map<Set<Integer>, Integer> compare(List<OutlierClassificationResult> results) {
        for(OutlierClassificationResult d : results) {
            Set<Integer> as = Sets.newHashSet(d.getDatum().getAttributes());
            for(Set<Integer> ss : Sets.powerSet(as)) {
                if(ss.size() == 0) {
                    continue;
                }

                cnt.compute(ss, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        return cnt;
    }
}
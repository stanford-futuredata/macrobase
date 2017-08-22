package edu.stanford.futuredata.macrobase.analysis.summary.count;

import com.google.common.collect.Sets;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.ExactCount;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class ExactCountTest {
    @Test
    public void testCount() {
        ExactCount ec = new ExactCount();
        HashMap<Integer, Integer> truth = new HashMap<>();

        List<Set<Integer>> its = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < i; ++j) {
                its.add(Sets.newHashSet(i));
                truth.compute(i, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        ec.count(its);

        for (Map.Entry<Integer, Double> cnt : ec.getCounts().entrySet()) {
            assertEquals(truth.get(cnt.getKey()), cnt.getValue(), 1e-10);
        }
    }
}

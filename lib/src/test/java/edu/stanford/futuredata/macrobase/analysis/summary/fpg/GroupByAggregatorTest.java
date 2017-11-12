package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class GroupByAggregatorTest {
    @Test
    public void testSimple() {
        GroupByAggregator gb = new GroupByAggregator();
        List<Set<Integer>> rows = new ArrayList<>();
        rows.add(new HashSet<>(Arrays.asList(1,2)));
        rows.add(new HashSet<>(Arrays.asList(1,3)));
        rows.add(new HashSet<>(Arrays.asList(1,2)));
        List<ItemsetWithCount> grouped = gb.groupBy(rows);
        assertEquals(2, grouped.size());
    }

}
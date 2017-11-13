package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ExactCountTest {
    @Test
    public void benchExactCount() {
        Set<Integer> s = new HashSet<>();
        s.add(1);
        s.add(2);
        ItemsetWithCount ic = new ItemsetWithCount(s, 1);
        List<Set<Integer>> rawRows = new ArrayList<>();
        List<ItemsetWithCount> rawGrouped = new ArrayList<>();

        int n = 100000;
        for(int i = 0; i < n; i++) {
            rawRows.add(s);
            rawGrouped.add(ic);
        }

        ExactCount e= new ExactCount();
        long startTime = System.currentTimeMillis();
        e.count(rawRows);
        long time1 = System.currentTimeMillis();
        e.countGrouped(rawGrouped);
        long time2 = System.currentTimeMillis();
        System.out.println(time1-startTime);
        System.out.println(time2-time1);
    }

}
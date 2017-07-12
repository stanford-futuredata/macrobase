package edu.stanford.futuredata.macrobase.analysis.summary.itemset;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class IntSetTest {
    @Test
    public void testSimple() {
        IntSet s1 = new IntSet(1, 2);
        Set<Integer> s2 = new HashSet<>(2);
        s2.add(1);
        s2.add(2);
        assertEquals(s2, s1.getSet());
        assertEquals(2, s1.size());
        assertEquals(s1, s1);
        assertTrue(s1.toString().length() > 0);
    }

}
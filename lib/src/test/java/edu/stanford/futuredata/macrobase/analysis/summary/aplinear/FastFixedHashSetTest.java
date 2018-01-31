package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.FastFixedHashSet;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class FastFixedHashSetTest {
    @Test
    public void testSimple() {
        FastFixedHashSet set = new FastFixedHashSet(16);
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(18);
        set.add(4);
        set.add(19);
        assertTrue(set.contains((long) 1));
        assertTrue(set.contains((long) 2));
        assertTrue(set.contains((long) 3));
        assertTrue(set.contains((long) 18));
        assertTrue(set.contains((long) 4));
        assertTrue(set.contains((long) 19));
        assertEquals(16, set.getCapacity());
    }
}

package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsArray;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class IntSetAsArrayTest {
    @Test
    public void testSimple() {
        IntSetAsArray s1 = new IntSetAsArray(1, 2);
        Set<Integer> s2 = new HashSet<>(2);
        s2.add(1);
        s2.add(2);
        assertEquals(s2, s1.getSet());
        assertEquals(s1, s1);
        assertTrue(s1.toString().length() > 0);
    }

    @Test
    public void testContains() {
        IntSetAsArray s1 = new IntSetAsArray(1,2);
        assertTrue(s1.contains(1));
        assertTrue(s1.contains(2));
        IntSetAsArray s2 = new IntSetAsArray(1,2,3);
        assertTrue(s2.contains(3));
        IntSetAsArray s3 = new IntSetAsArray(1,4);
        assertFalse(s3.contains(2));
        assertTrue(s3.contains(4));
    }
}
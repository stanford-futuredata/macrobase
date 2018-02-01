package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsLong;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IntSetAsLongTest {
    @Test
    public void testSimple() {
        IntSet s1 = new IntSetAsLong(1, 3, 20);
        assertEquals(20, s1.getFirst());
        assertEquals(3, s1.getSecond());
        assertEquals(1, s1.getThird());
        assertTrue(s1.contains(1));
        assertTrue(s1.contains(3));
        assertTrue(s1.contains(20));
        IntSet p1 = new IntSetAsLong(5234, 2342);
        IntSet p2 = new IntSetAsLong(2342, 5234);
        assertEquals(p1, p2);
        assertEquals(p1.getFirst(), 5234);
        assertEquals(p1.getSecond(), 2342);
        assertEquals(p1.getThird(), 0);
    }
}

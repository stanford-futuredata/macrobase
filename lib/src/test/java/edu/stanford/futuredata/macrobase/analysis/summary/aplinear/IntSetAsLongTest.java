package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IntSetAsLongTest {
    @Test
    public void testSimple() {
        long s1 = IntSetAsLong.threeIntToLong(1, 3, 20);
        assertTrue(IntSetAsLong.contains(s1, 1));
        assertTrue(IntSetAsLong.contains(s1, 20));
        assertTrue(IntSetAsLong.contains(s1, 3));
        assertEquals(IntSetAsLong.getFirst(s1), 20);
        assertEquals(IntSetAsLong.getSecond(s1), 3);
        assertEquals(IntSetAsLong.getThird(s1), 1);
        long p1 = IntSetAsLong.twoIntToLong(5234, 2342);
        long p2 = IntSetAsLong.twoIntToLong(2342, 5234);
        assertEquals(p1, p2);
    }
}

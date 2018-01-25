package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IntSetAsLongTest {
    @Test
    public void testSimple() {
        long s1 = IntSetAsLong.threeIntToLong(1, 20, 3, 5);
        long s2 = IntSetAsLong.threeIntToLong(3, 20, 1, 5);
        long s3 = IntSetAsLong.threeIntToLong(3, 1, 20, 5);
        assertEquals(s1, s2);
        assertEquals(s1, s3);
        assertTrue(IntSetAsLong.contains(s1, 1, 5));
        assertTrue(IntSetAsLong.contains(s1, 20, 5));
        assertTrue(IntSetAsLong.contains(s1, 3, 5));
        assertEquals(IntSetAsLong.getFirst(s1, 5), 20);
        assertEquals(IntSetAsLong.getSecond(s1, 5), 3);
        assertEquals(IntSetAsLong.getThird(s1, 5), 1);
        long p1 = IntSetAsLong.twoIntToLong(5234, 2342, 5);
        long p2 = IntSetAsLong.twoIntToLong(2342, 5234, 5);
        assertEquals(p1, p2);
        long oneOneOne = IntSetAsLong.threeIntToLong(1, 1, 1, 1);
        assertEquals(oneOneOne, 7);
        long oneOne = IntSetAsLong.twoIntToLong(1, 1, 1);
        assertEquals(oneOne, 3);
    }
}

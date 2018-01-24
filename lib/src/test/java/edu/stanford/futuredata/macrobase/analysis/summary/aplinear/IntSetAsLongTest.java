package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.apriori.IntSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IntSetAsLongTest {
    @Test
    public void testSimple() {
        long s1 = IntSetAsLong.threeIntToLong(1, 20, 3);
        long s2 = IntSetAsLong.threeIntToLong(3, 20, 1);
        long s3 = IntSetAsLong.threeIntToLong(3, 1, 20);
        assertEquals(s1, s2);
        assertEquals(s1, s3);
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

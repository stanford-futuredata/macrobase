package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.FastFixedHashTable;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class FastFixedHashTableTest {

    @Test
    public void testSimple() {
        FastFixedHashTable table = new FastFixedHashTable(16, 1);
        double[] putArrayOne = {1.0};
        double[] putArrayTwo = {2.0};
        double[] putArrayThree = {3.0};
        double[] putArrayFour = {4.0};
        double[] putArrayFive = {5.0};
        double[] putArraySix = {6.0};
        table.put(1, putArrayOne);
        table.put(2, putArrayTwo);
        table.put(3, putArrayThree);
        table.put(18, putArrayFour);
        table.put(4, putArrayFive);
        table.put(19, putArraySix);
        assertEquals(1.0, table.get(1)[0], 0.01);
        assertEquals(2.0, table.get(2)[0], 0.01);
        assertEquals(3.0, table.get(3)[0], 0.01);
        assertEquals(4.0, table.get(18)[0], 0.01);
        assertEquals(5.0, table.get(4)[0], 0.01);
        assertEquals(6.0, table.get(19)[0], 0.01);
        assertTrue(table.keySet().contains((long) 1));
        assertTrue(table.keySet().contains((long) 2));
        assertTrue(table.keySet().contains((long) 3));
        assertTrue(table.keySet().contains((long) 18));
        assertTrue(table.keySet().contains((long) 4));
        assertTrue(table.keySet().contains((long) 19));
    }
}

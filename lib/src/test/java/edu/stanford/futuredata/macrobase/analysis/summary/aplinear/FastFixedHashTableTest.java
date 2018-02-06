package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.FastFixedHashTable;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsArray;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsLong;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class FastFixedHashTableTest {

    @Test
    public void testSimple() {
        FastFixedHashTable table = new FastFixedHashTable(16, 1, true);
        assertEquals(16, table.getCapacity());
        double[] putArrayOne = {1.0};
        double[] putArrayTwo = {2.0};
        double[] putArrayThree = {3.0};
        table.put(new IntSetAsArray(1), putArrayOne);
        table.put(new IntSetAsArray(2), putArrayTwo);
        table.put(new IntSetAsArray(18), putArrayThree);
        assertEquals(1.0, table.get(new IntSetAsArray(1))[0], 0.01);
        assertEquals(2.0, table.get(new IntSetAsArray(2))[0], 0.01);
        assertEquals(3.0, table.get(new IntSetAsArray(18))[0], 0.01);
        FastFixedHashTable tableTwo = new FastFixedHashTable(16, 1, false);
        tableTwo.put(new IntSetAsLong(1), putArrayOne);
        tableTwo.put(new IntSetAsLong(2), putArrayTwo);
        tableTwo.put(new IntSetAsLong(18), putArrayThree);
        assertEquals(1.0, tableTwo.get(new IntSetAsLong(1))[0], 0.01);
        assertEquals(2.0, tableTwo.get(new IntSetAsLong(2))[0], 0.01);
        assertEquals(3.0, tableTwo.get(new IntSetAsLong(18))[0], 0.01);
    }
}

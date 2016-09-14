package macrobase.analysis.contextualoutlier;

import macrobase.datamodel.Datum;
import macrobase.util.BitSetUtil;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;

public class ContextTest {

    @Test
    public void bitSetTest() {
        BitSet bs1 = new BitSet(4); //0011
        bs1.set(2);
        bs1.set(3);
        
        BitSet bs2 = new BitSet(4); //0101
        bs2.set(1);
        bs2.set(3);
        
        BitSet bs1Clone = (BitSet)bs1.clone();
        bs1Clone.andNot(bs2);
        assertEquals(bs1Clone.get(0), false);
        assertEquals(bs1Clone.get(1), false);
        assertEquals(bs1Clone.get(2), true);
        assertEquals(bs1Clone.get(3), false);
    }
    
    @Test
    public void contextTest() {
        Datum d1 = new Datum(Arrays.asList(1),1.0);
        Datum d2 = new Datum(Arrays.asList(2),2.0);
        Datum d3 = new Datum(Arrays.asList(3),3.0);
        Datum d4 = new Datum(Arrays.asList(4),4.0);
        Datum d5 = new Datum(Arrays.asList(5),5.0);
        List<Datum> data = new ArrayList<Datum>();
        data.add(d1);
        data.add(d2);
        data.add(d3);
        data.add(d4);
        data.add(d5);
        
        List<Integer> sampleIndexes = new ArrayList<Integer>();
        sampleIndexes.add(0);
        sampleIndexes.add(2);
        sampleIndexes.add(4);
        
        Context globalContext = new Context(data, null);
        globalContext.setBitSet(data);
        Interval interval0 = new IntervalDiscrete(0, "C0", 0);
        Interval interval1 = new IntervalDiscrete(0, "C0", 1);
        Interval interval3 = new IntervalDouble(1, "C1", 0, 6);
        Interval interval4 = new IntervalDouble(2, "C2", 0, 6);
        
        //The bitset is moot now, just to test the joinability
        Context c0 = new Context(0,interval0,globalContext);
        BitSet bs0 = new BitSet();
        c0.setBitSet(bs0);
        
        Context c1 = new Context(0,interval1,globalContext);
        BitSet bs1 = new BitSet();
        c1.setBitSet(bs1);
        
        Context c3 = new Context(1,interval3,globalContext);
        BitSet bs3 = new BitSet();
        c3.setBitSet(bs3);
        
        Context c4 = new Context(2,interval4,globalContext);
        BitSet bs4 = new BitSet();
        c4.setBitSet(bs4);
        
        ContextIndexTree cit = new ContextIndexTree();
        cit.addContext(c0);
        cit.addContext(c1);
        cit.addContext(c3);
        cit.addContext(c4);
        
        Context c03 = c0.join(c3, data, 0, cit);
        assertEquals(c03.getParents().contains(c0),true);
        assertEquals(c03.getParents().contains(c3),true);
        
        Context c04 = c0.join(c4, data, 0, cit);
        assertEquals(c04.getParents().contains(c0),true);
        assertEquals(c04.getParents().contains(c4),true);
        
        Context c13 = c1.join(c3, data, 0, cit);
        assertEquals(c13.getParents().contains(c1),true);
        assertEquals(c03.getParents().contains(c3),true);
        
        Context c34 = c3.join(c4, data, 0, cit);
        assertEquals(c34.getParents().contains(c3),true);
        assertEquals(c34.getParents().contains(c4),true);
        
        cit = new ContextIndexTree();
        cit.addContext(c03);
        cit.addContext(c13);
        cit.addContext(c04);
        
        Context c03_13 = c03.join(c13, data, 0, cit);
        assertEquals(c03_13,null);
        
        Context c03_3 = c03.join(c3, data, 0, cit);
        assertEquals(c03_3,null);
        
        Context c03_4 = c03.join(c04, data, 0, cit);
        assertEquals(c03_4,null);
        
        cit.addContext(c34);
        c03_4 = c03.join(c04, data, 0, cit);
        assertNotEquals(c03_4,null);

    }
}

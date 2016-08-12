package macrobase.analysis.contextualoutlier;

import macrobase.datamodel.Datum;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class ContextTest {

    @Test
    public void contextTest() {
        ContextualDatum d1 = new ContextualDatum(Arrays.asList(1),1.0);
        ContextualDatum d2 = new ContextualDatum(Arrays.asList(2),2.0);
        ContextualDatum d3 = new ContextualDatum(Arrays.asList(3),3.0);
        ContextualDatum d4 = new ContextualDatum(Arrays.asList(4),4.0);
        ContextualDatum d5 = new ContextualDatum(Arrays.asList(5),5.0);
        List<ContextualDatum> data = new ArrayList<>();
        data.add(d1);
        data.add(d2);
        data.add(d3);
        data.add(d4);
        data.add(d5);
        
        HashSet<ContextualDatum> sample = new HashSet<>();
        sample.add(d1);
        sample.add(d3);
        sample.add(d5);
        
        Context globalContext = new Context(sample, false, false, 0.05);
        
        Interval interval0 = new IntervalDiscrete(0, "C0", 0);
        Interval interval1 = new IntervalDiscrete(0, "C0", 1);
        Interval interval2 = new IntervalDiscrete(0, "C0", 2);
        Interval interval3 = new IntervalDouble(1, "C1", 0, 6);
        Context c0 = new Context(0,interval0,globalContext);
        Context c1 = new Context(0,interval1,globalContext);
        Context c2 = new Context(0,interval2,globalContext);
        Context c3 = new Context(0,interval3,globalContext);

        Context c03 = c0.join(c3, data, 0);
        assertEquals(c03.getParents().contains(c0),true);
        assertEquals(c03.getParents().contains(c3),true);
        
        Context c13 = c1.join(c3, data, 0);
        assertEquals(c13.getParents().contains(c1),true);
        assertEquals(c03.getParents().contains(c3),true);
        
        Context c03_13 = c03.join(c13, data, 0);
        assertEquals(c03_13,null);
        
        Context c03_3 = c03.join(c3, data, 0);
        assertEquals(c03_3,null);

    }
}

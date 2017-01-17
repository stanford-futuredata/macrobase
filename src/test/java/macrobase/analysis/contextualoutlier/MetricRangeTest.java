package macrobase.analysis.contextualoutlier;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class MetricRangeTest {

    @Test
    public void metricRangeIntersectTest() {
        MetricRange mr1 = new MetricRange(1,5, true, true);
        MetricRange mr2 = new MetricRange(2,4, true, true);
        mr1.intersect(mr2);
        assertEquals(mr1.isSame(new MetricRange(2,4,true,true)), true);
        
        mr1 = new MetricRange(1,5, true, false);
        mr2 = new MetricRange(1,6, false, true);
        mr1.intersect(mr2);
        assertEquals(mr1.isSame(new MetricRange(1,5,false,false)), true);
        
        mr1 = new MetricRange(1,5, true, false);
        mr2 = new MetricRange(1,5, false, true);
        mr1.intersect(mr2);
        assertEquals(mr1.isSame(new MetricRange(1,5,false,false)), true);
    }
    
    @Test
    public void metricContainedTest() {
        MetricRange mr1 = new MetricRange(1,5, true, true);
        MetricRange mr2 = new MetricRange(2,4, true, true);
        assertEquals(mr1.contained(mr2), false);
       
        mr1 = new MetricRange(1,5, true, true);
        mr2 = new MetricRange(1,5, true, true);
        assertEquals(mr1.contained(mr2), true);
        
        mr1 = new MetricRange(1,5, true, true);
        mr2 = new MetricRange(1,5, true, false);
        assertEquals(mr1.contained(mr2), false);
        
        mr1 = new MetricRange(1,5, true, true);
        mr2 = new MetricRange(1,5, false, true);
        assertEquals(mr1.contained(mr2), false);

    }
    
    @Test
    public void metricInRangeTest() {
        MetricRange mr1 = new MetricRange(1,5, true, true);
        assertEquals(mr1.isInRange(3), true);

        mr1 = new MetricRange(3,5, true, true);
        assertEquals(mr1.isInRange(3), true);
        
        mr1 = new MetricRange(1,5, true, true);
        assertEquals(mr1.isInRange(5), true);
        
        mr1 = new MetricRange(1,5, true, false);
        assertEquals(mr1.isInRange(5), false);
        
        mr1 = new MetricRange(1,5, true, false);
        assertEquals(mr1.isInRange(6), false);
    }
    
    @Test
    public void metricMinusTest() {
        MetricRange mr1 = new MetricRange(1,5, true, true);
        MetricRange mr2 = new MetricRange(2,4, true, true);
        List<MetricRange> result = mr1.minus(mr2);
        assertEquals(result.get(0).isSame(new MetricRange(1,2,true,false)), true);
        assertEquals(result.get(1).isSame(new MetricRange(4,5,false,true)), true);
        
        mr1 = new MetricRange(1,5, true, true);
        mr2 = new MetricRange(1,4, true, true);
        result = mr1.minus(mr2);
        assertEquals(result.get(0).isSame(new MetricRange(4,5,false,true)), true);
        
        mr1 = new MetricRange(2,5, true, true);
        mr2 = new MetricRange(1,5, true, true);
        result = mr1.minus(mr2);
        assertEquals(result.size(),0);
        
        mr1 = new MetricRange(1,5, false, false);
        mr2 = new MetricRange(1,5, true, true);
        result = mr1.minus(mr2);
        assertEquals(result.size(),0);
        
        mr1 = new MetricRange(1,5, true, true);
        mr2 = new MetricRange(2,5, false, true);
        result = mr1.minus(mr2);
        assertEquals(result.get(0).isSame(new MetricRange(1,2,true,true)), true);
        
        mr1 = new MetricRange(1,5, true, true);
        mr2 = new MetricRange(2,6, false, true);
        result = mr1.minus(mr2);
        assertEquals(result.get(0).isSame(new MetricRange(1,2,true,true)), true);
      
        mr1 = new MetricRange(1,5, true, true);
        mr2 = new MetricRange(2,4, false, false);
        result = mr1.minus(mr2);
        assertEquals(result.get(0).isSame(new MetricRange(1,2,true,true)), true);
        assertEquals(result.get(1).isSame(new MetricRange(4,5,true,true)), true);
        
        mr1 = new MetricRange(1,5, false, true);
        mr2 = new MetricRange(2,4, false, true);
        result = mr1.minus(mr2);
        assertEquals(result.get(0).isSame(new MetricRange(1,2,false,true)), true);
        assertEquals(result.get(1).isSame(new MetricRange(4,5,false,true)), true);

    }
    
    
}

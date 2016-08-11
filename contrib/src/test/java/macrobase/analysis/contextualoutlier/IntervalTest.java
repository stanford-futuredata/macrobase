package macrobase.analysis.contextualoutlier;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

import macrobase.ingest.DatumEncoder;

public class IntervalTest {

   @Test
   public void intervalDiscreteTest() {
       Interval interval = new IntervalDiscrete(0, "testColumn", 1);
       DatumEncoder e = new DatumEncoder();
       e.recordAttributeName(0, "testColumn");
       int encoded0 = e.getIntegerEncoding(0, "attrValue0");
       int encoded1 = e.getIntegerEncoding(0, "attrValue1");
       Assert.assertEquals(encoded0, 0);
       Assert.assertEquals(encoded1, 1);
       assertEquals(interval.print(e).contains("attrValue1"),true);
   }
   
   @Test
   public void intervalDoubleTest() {
       Interval interval = new IntervalDouble(0, "testColumn", 1.0, 2.0);
       DatumEncoder e = new DatumEncoder();
       assertEquals(interval.print(e).contains("1.0"),true);
       assertEquals(interval.print(e).contains("2.0"),true);
   }

}

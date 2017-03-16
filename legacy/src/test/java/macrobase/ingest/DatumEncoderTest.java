package macrobase.ingest;

import org.junit.Test;

import static org.junit.Assert.*;

public class DatumEncoderTest {
    @Test
    public void getSetTest() {
        DatumEncoder e = new DatumEncoder();
        e.recordAttributeName(0, "foo");
        int encoded1 = e.getIntegerEncoding(0, "attrValue1");
        int encoded2 = e.getIntegerEncoding(0, "attrValue2");
        int encoded2plus = e.getIntegerEncoding(0, "attrValue2");

        assertEquals(encoded2, encoded2plus);
        assertEquals("foo", e.getAttribute(encoded1).getColumn());
        assertEquals("foo", e.getAttribute(encoded2).getColumn());
        assertEquals("attrValue2", e.getAttribute(encoded2).getValue());
    }

    @Test
    public void copyTest() {
        DatumEncoder e1 = new DatumEncoder();
        e1.recordAttributeName(0, "foo");
        int encoded1 = e1.getIntegerEncoding(0, "attrValue1");
        int encoded2 = e1.getIntegerEncoding(0, "attrValue2");
        e1.recordAttributeName(1, "bar");
        int encoded3 = e1.getIntegerEncoding(1, "attrValue3");

        DatumEncoder e2 = new DatumEncoder();
        e2.copy(e1);

        assertEquals("foo", e2.getAttribute(encoded1).getColumn());
        assertEquals("bar", e2.getAttribute(encoded3).getColumn());
        assertEquals("attrValue3", e2.getAttribute(encoded3).getValue());

        int encoded4 = e2.getIntegerEncoding(1, "attrValue4");
        assertEquals("attrValue4", e2.getAttribute(encoded4).getValue());
    }
}

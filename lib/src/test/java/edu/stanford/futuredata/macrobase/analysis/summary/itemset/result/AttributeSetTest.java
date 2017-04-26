package edu.stanford.futuredata.macrobase.analysis.summary.itemset.result;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class AttributeSetTest {
    @Test
    public void testSimple() throws Exception {
        Map<String, String> v1 = new HashMap<>();
        v1.put("c1", "val1");
        v1.put("c2", "val2");

        Map<String, String> v2 = new HashMap<>();
        v2.put("c2", "val2");

        AttributeSet a1 = new AttributeSet(
                .2, 10.0, 21.2, v1
        );
        AttributeSet a2 = new AttributeSet(
                .7, 30.0, 25.0, v2
        );

        assertTrue(a1.contains(a2));
    }

}
package edu.stanford.futuredata.macrobase.analysis.summary.fpg.result;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class FPGAttributeSetTest {
    @Test
    public void testSimple() throws Exception {
        Map<String, String> v1 = new HashMap<>();
        v1.put("c1", "val1");
        v1.put("c2", "val2");

        Map<String, String> v2 = new HashMap<>();
        v2.put("c2", "val2");

        FPGAttributeSet a1 = new FPGAttributeSet(
                .2, 10.0, 21.2, v1
        );
        FPGAttributeSet a2 = new FPGAttributeSet(
                .7, 30.0, 25.0, v2
        );

        assertTrue(a1.contains(a2));
    }

}
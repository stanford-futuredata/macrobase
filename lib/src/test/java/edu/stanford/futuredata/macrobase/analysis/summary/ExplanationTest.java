package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ExplanationTest {
    @Test
    public void testPrune() throws Exception {
        Map<String, String> v1 = new HashMap<>();
        v1.put("c1", "val1");
        v1.put("c2", "val2");

        Map<String, String> v2 = new HashMap<>();
        v2.put("c2", "val2");

        AttributeSet a1 = new AttributeSet(
                .2, 10.0, 21.2, v1
        );
        // a2 makes a1 redundant
        AttributeSet a2 = new AttributeSet(
                .7, 30.0, 25.0, v2
        );

        Explanation e = new Explanation(
                Arrays.asList(a1, a2),
                100,
                10,
                0
        );
        assertEquals(2, e.getItemsets().size());
        assertEquals(1, e.prune().getItemsets().size());

    }

}
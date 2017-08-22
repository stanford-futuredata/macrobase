package edu.stanford.futuredata.macrobase.analysis.summary.fpg.result;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
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

        FPGAttributeSet a1 = new FPGAttributeSet(
                .2, 10.0, 21.2, v1
        );
        // a2 makes a1 redundant
        FPGAttributeSet a2 = new FPGAttributeSet(
                .7, 30.0, 25.0, v2
        );

        FPGExplanation e = new FPGExplanation(
                Arrays.asList(a1, a2),
                100,
                10,
                0
        );
        assertEquals(2, e.getItemsets().size());
        assertEquals(1, e.prune().getItemsets().size());

    }
}
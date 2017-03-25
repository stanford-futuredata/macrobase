package macrobase.analysis.summary.itemset;

import com.google.common.collect.Lists;

import macrobase.analysis.summary.itemset.Apriori;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class RiskRatioTest {

    @Test
    public void testRatio() {
        assertEquals(1.0, RiskRatio.compute(10, 10, 100, 100), 0.01);
        assertEquals(6., RiskRatio.compute(10, 10, 1000, 100), 0.01);
        assertEquals(900.082, RiskRatio.compute(10, 99, 1000, 100), 0.01);
    }

    @Test
    public void testRatioBoundaryConditions() {
        // no exposure
        assertEquals(0, RiskRatio.compute(0, 0, 100, 100), 0);

        // all exposed
        assertEquals(0, RiskRatio.compute(100, 100, 100, 100), 0);

        // event only found in exposed
        assertEquals(Double.POSITIVE_INFINITY, RiskRatio.compute(0, 100, 100, 100), 0);
        assertEquals(Double.POSITIVE_INFINITY, RiskRatio.compute(null, 100, 100, 100), 0);

        // event never found in exposed
        assertEquals(0, RiskRatio.compute(100, 0, 1000, 100), 0);
        assertEquals(0, RiskRatio.compute(100, null, 1000, 100), 0);

        // handling nulls, all zeroes
        assertEquals(0, RiskRatio.compute(null, null, null, null), 0);
    }
}

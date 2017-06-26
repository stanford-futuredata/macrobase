package macrobase.analysis.summary.itemset;

import org.junit.Test;


import static org.junit.Assert.assertEquals;

public class RiskRatioTest {

    @Test
    public void testRatio() {
        assertEquals(1.0, RiskRatio.compute(10, 10, 100, 100).get(), 0.01);
        assertEquals(6., RiskRatio.compute(10, 10, 1000, 100).get(), 0.01);
        assertEquals(900.082, RiskRatio.compute(10, 99, 1000, 100).get(), 0.01);
    }

    @Test
    public void testRatioBoundaryConditions() {
        // no exposure
        assertEquals(0, RiskRatio.compute(0, 0, 100, 100).get(), 0);

        // all exposed
        assertEquals(0, RiskRatio.compute(100, 100, 100, 100).get(), 0);

        // event only found in exposed
        assertEquals(Double.POSITIVE_INFINITY, RiskRatio.compute(0, 100, 100, 100).get(), 0);
        assertEquals(Double.POSITIVE_INFINITY, RiskRatio.compute(null, 100, 100, 100).get(), 0);

        // event never found in exposed
        assertEquals(0, RiskRatio.compute(100, 0, 1000, 100).get(), 0);
        assertEquals(0, RiskRatio.compute(100, null, 1000, 100).get(), 0);

        // handling nulls, all zeroes
        assertEquals(0, RiskRatio.compute(null, null, null, null).get(), 0);
    }
}

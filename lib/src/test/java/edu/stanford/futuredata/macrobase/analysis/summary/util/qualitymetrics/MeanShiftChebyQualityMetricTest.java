package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import static org.junit.Assert.*;
import static junit.framework.TestCase.assertEquals;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MeanShiftChebyQualityMetricTest {
    @Test
    public void testSimple() {
        MeanShiftChebyQualityMetric m = new MeanShiftChebyQualityMetric(
                0, 1, 2, 3, 4, 5
        );
        double[] globalAggregates = {100, 100, 100, 100, 1000, 1000};
        m.initialize(globalAggregates);
        List<Double> summarizerThresholds = Arrays.asList(.2, .2);
        m.setSupportThresholdIdxs(0, 1);
        m.setAPLThresholdsForOptimization(summarizerThresholds);

        double[] targetSubgroup = {40, 40, 100, 100, 260, 260};
        double currentMeanShift = m.value(targetSubgroup);
        double maxPossibleMeanShift = m.maxSubgroupValue(targetSubgroup);

        assertEquals(1.0, currentMeanShift);
        assertTrue(maxPossibleMeanShift < 2.0);
    }
}
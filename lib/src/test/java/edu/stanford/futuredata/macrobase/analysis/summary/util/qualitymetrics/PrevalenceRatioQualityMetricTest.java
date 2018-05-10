package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;


import static junit.framework.TestCase.assertEquals;

import org.junit.Test;

public class PrevalenceRatioQualityMetricTest {

    // Taken from the old PrevalenceRatio code
    private static double calc(
        double matchedOutliers,
        double matchedTotal,
        double outlierCount,
        double totalCount) {
        if (outlierCount == 0 || matchedOutliers == 0) {
            return 0;
        }

        double inlierCount = totalCount - outlierCount;
        double matchedInlier = matchedTotal - matchedOutliers;

        if (matchedInlier == 0) {
            matchedInlier += 1; // increment by 1 to avoid DivideByZero error
        }

        return (matchedOutliers / outlierCount) / (matchedInlier / inlierCount);
    }

    @Test
    public void test1() {
        final double matchedOutliers = 10.0;
        final double matchedTotal = 11.0;
        final double outlierCount = 25.0;
        final double totalCount = 50.0;
        testPrevalenceRatio(matchedOutliers, matchedTotal, outlierCount, totalCount);
    }

    @Test
    public void test2() {
        final double matchedOutliers = 10.0;
        final double matchedTotal = 10.0;
        final double outlierCount = 25.0;
        final double totalCount = 50.0;
        testPrevalenceRatio(matchedOutliers, matchedTotal, outlierCount, totalCount);
    }

    @Test
    public void test3() {
        final double matchedOutliers = 10.0;
        final double matchedTotal = 11.0;
        final double outlierCount = 10.0;
        final double totalCount = 50.0;
        testPrevalenceRatio(matchedOutliers, matchedTotal, outlierCount, totalCount);
    }

    @Test
    public void test4() {
        final double matchedOutliers = 10.0;
        final double matchedTotal = 11.0;
        final double outlierCount = 10.0;
        final double totalCount = 10.0;
        testPrevalenceRatio(matchedOutliers, matchedTotal, outlierCount, totalCount);
    }

    private void testPrevalenceRatio(final double matchedOutliers, final double matchedTotal,
        final double outlierCount, final double totalCount) {
        final PrevalenceRatioQualityMetric qMetric = new PrevalenceRatioQualityMetric(0, 1);

        qMetric.initialize(new double[]{outlierCount, totalCount});
        assertEquals(calc(matchedOutliers, matchedTotal, outlierCount, totalCount),
            qMetric.value(new double[]{matchedOutliers, matchedTotal}));
    }

}
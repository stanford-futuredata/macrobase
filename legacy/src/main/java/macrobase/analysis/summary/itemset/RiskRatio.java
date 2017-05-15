package macrobase.analysis.summary.itemset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiskRatio {
    private static final Logger log = LoggerFactory.getLogger(RiskRatio.class);

    private static double computeDouble(double exposedInlierCount,
                                        double exposedOutlierCount,
                                        double totalInliers,
                                        double totalOutliers) {
        double totalExposedCount = exposedInlierCount + exposedOutlierCount;
        double totalMinusExposedCount = totalInliers + totalOutliers - totalExposedCount;
        double unexposedOutlierCount = (totalOutliers - exposedOutlierCount);
        double unexposedInlierCount = (totalInliers - exposedInlierCount);

        // no exposure occurred
        if (totalExposedCount == 0) {
            log.error("Computing risk ratio with no exposure.");
            return 0;
        }

        // we only exposed this ratio, everything matched!
        if (totalMinusExposedCount == 0) {
            return 0;
        }

        // all outliers had this pattern
        if (unexposedOutlierCount == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double z = 2.0;
        double correction = z*Math.sqrt(
                (exposedInlierCount / exposedOutlierCount)/totalExposedCount
                + (unexposedInlierCount / unexposedInlierCount)/totalMinusExposedCount
        );

        return (exposedOutlierCount / totalExposedCount) /
               (unexposedOutlierCount / totalMinusExposedCount) - correction;
    }

    public static double compute(Number exposedInlierCount,
                                 Number exposedOutlierCount,
                                 Number totalInliers,
                                 Number totalOutliers) {
        if(exposedInlierCount == null) {
            exposedInlierCount = 0.;
        }

        if(exposedOutlierCount == null) {
            exposedOutlierCount = 0.;
        }

        if(totalInliers == null) {
            totalInliers = 0.;
        }

        if(totalOutliers == null) {
            totalOutliers = 0.;
        }

        return computeDouble(exposedInlierCount.doubleValue(),
                             exposedOutlierCount.doubleValue(),
                             totalInliers.doubleValue(),
                             totalOutliers.doubleValue());
    }
}

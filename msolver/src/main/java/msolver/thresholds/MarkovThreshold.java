package msolver.thresholds;

import msolver.util.MathUtil;
import msolver.struct.MomentStruct;

public class MarkovThreshold implements MomentThreshold {
    private MomentStruct ms;

    public MarkovThreshold(
            MomentStruct ms
    ) {
        this.ms = ms;
    }

    @Override
    public double[] bound(double cutoff) {
        double[] outlierRateBounds = new double[2];
        outlierRateBounds[0] = 0.0;
        outlierRateBounds[1] = 1.0;

        int ka = ms.powerSums.length;
        int kb = ms.logSums.length;
        double n = ms.powerSums[0];
        if (ka > 1) {
            double[] xMinusMinMoments = MathUtil.shiftPowerSum(ms.powerSums, 1, ms.min);
            double[] maxMinusXMoments = MathUtil.shiftPowerSum(ms.powerSums, -1, ms.max);
            for (int i = 1; i < ka; i++) {
                double outlierRateUpperBound = (xMinusMinMoments[i] / n) / Math.pow(cutoff - ms.min, i);
                double outlierRateLowerBound = 1.0 - (maxMinusXMoments[i] / n) / Math.pow(ms.max - cutoff, i);
                outlierRateBounds[0] = Math.max(outlierRateBounds[0], outlierRateLowerBound);
                outlierRateBounds[1] = Math.min(outlierRateBounds[1], outlierRateUpperBound);
            }
        }

        double nl = ms.logSums[0];
        if (kb > 1 && nl != 0) {
            double logCutoff = Math.log(cutoff);
            double fracIncluded = nl / n;
            double[] xMinusMinLogMoments = MathUtil.shiftPowerSum(ms.logSums, 1, ms.logMin);
            double[] maxMinusXLogMoments = MathUtil.shiftPowerSum(ms.logSums, -1, ms.logMax);
            for (int i = 1; i < kb; i++) {
                double outlierRateUpperBound = (
                        (1.0 - fracIncluded) +
                                fracIncluded * (xMinusMinLogMoments[i] / nl) / Math.pow(logCutoff - ms.logMin, i)
                );
                double outlierRateLowerBound = (
                        1.0 -
                                fracIncluded * (maxMinusXLogMoments[i] / nl) / Math.pow(ms.logMax - logCutoff, i)
                );
                outlierRateBounds[0] = Math.max(outlierRateBounds[0], outlierRateLowerBound);
                outlierRateBounds[1] = Math.min(outlierRateBounds[1], outlierRateUpperBound);
            }
        }

        return outlierRateBounds;
    }
}

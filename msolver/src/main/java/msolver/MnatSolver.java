package msolver;

import msolver.util.MathUtil;

import java.util.Arrays;
import java.util.List;

/**
 * https://www.sciencedirect.com/science/article/pii/S0167715208000539
 */
public class MnatSolver {
    // values expected to lie in [0,1]
    public static double[] estimatePDF(
            double[] moments
    ) {
        int a = moments.length - 1;
        double[] pdf = new double[a+1];
        long[][] binoms = MathUtil.getBinomials(a);

        for (int k = 0; k <= a; k++) {
            double sum = 0;
            for (int j = k; j <= a; j++) {
                double curTerm = (double)binoms[a][j]*(double)binoms[j][k]*moments[j];
                if ((j - k) % 2 != 0) {
                    curTerm *= -1;
                }
                sum += curTerm;
            }
            pdf[k] = sum;
        }
        return pdf;
    }

    public static double[] estimateCDF(
            double[] moments
    ) {
        double[] pdf = estimatePDF(moments);
        double[] cdf = new double[pdf.length];
        cdf[0] = pdf[0];
        for (int i = 1; i < pdf.length; i++) {
            cdf[i] = cdf[i-1] + pdf[i];
        }
        return cdf;
    }

    public static double[] estimateQuantiles(
            double min,
            double max,
            double[] powerSums,
            List<Double> ps
    ) {
        double[] moments = MathUtil.powerSumsToPosMoments(powerSums, min, max);
        double[] cdf = estimateCDF(moments);

        int n = ps.size();
        double[] qs = new double[n];
        int a = powerSums.length - 1;
        for (int i = 0; i < n; i++) {
            double p = ps.get(i);
            int idx = Arrays.binarySearch(cdf, p);
            if (idx < 0) {
                idx = -(idx + 1);
            }
            double fracIdx = 0.0;
            if (idx > 0) {
//                fracIdx = (idx-1.0)*1.0 + (p - cdf[idx - 1]) / (cdf[idx] - cdf[idx-1]);
                fracIdx = idx;
            }
            qs[i] = fracIdx/a * (max - min) + min;
        }
        return qs;
    }
}

package macrobase.analysis.stats.mixture;

public class MeanFieldVariationalInference {
    public static double[][] normalizeLogProbas(double[] lnMixing, double[] lnPrecision, double[][] dataLogLike) {
        double[][] r = new double[dataLogLike.length][lnMixing.length];
        for (int n = 0; n < dataLogLike.length; n++) {
            double normalizingConstant = 0;
            for (int k = 0; k < lnMixing.length; k++) {
                r[n][k] = Math.exp(lnMixing[k] + lnPrecision[k] + dataLogLike[n][k]);
                normalizingConstant += r[n][k];
            }
            for (int k = 0; k < lnMixing.length; k++) {
                if (normalizingConstant > 0) {
                    r[n][k] /= normalizingConstant;
                }
            }
        }
        return r;
    }
}

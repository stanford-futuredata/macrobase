package macrobase.analysis.stats.kde.kernel;

public class EpaKernel extends Kernel {
    @Override
    public double getDimFactor(int curDim) {
        return Math.pow(.75, dim);
    }

    @Override
    public double density(double[] d) {
        double prod = 1.0;
        for (int i=0; i < d.length; i++) {
            double del = d[i] * invBandwidth[i];
            if (del >= 1.0 || del <= -1.0) {
                return 0;
            }
            prod *= (1.0 - del * del);
        }
        return dimFactor * bwFactor * prod;
    }
}

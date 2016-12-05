package macrobase.analysis.stats.kde.kernel;

import org.apache.commons.math3.util.FastMath;

/**
 * Standard normal Gaussian with zero mean
 */
public class GaussianKernel extends Kernel {
    @Override
    public double getDimFactor(int curDim) {
        return Math.pow(2*Math.PI,-.5*curDim);
    }

    @Override
    public double density(double[] d) {
        double dist2 = 0.0;
        for (int i=0; i < d.length; i++) {
            double del = d[i] * invBandwidth[i];
            dist2 += del * del;
        }
        return dimFactor * bwFactor * FastMath.exp(-.5 * dist2);
    }
}

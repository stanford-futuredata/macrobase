package macrobase.analysis.stats.kernel;

import org.apache.commons.math3.linear.RealVector;

public class EpanchnikovMulticativeKernel extends Kernel {
    private int dimensions;
    private double norm;
    private double secondMoment;
    private final double secondMoment1D = 0.2;
    private final double norm1D = 0.6;

    public EpanchnikovMulticativeKernel(int dimensions) {
        this.dimensions = dimensions;
        this.norm = Math.pow(this.norm1D(), this.dimensions);
        this.secondMoment = Math.pow(this.secondMoment1D(), this.dimensions);
    }

    @Override
    public double density(RealVector u) {
        double rtn = 1.0;
        final int d = u.getDimension();
        for (int i = 0; i < d; i++) {
            double i2 = u.getEntry(i) * u.getEntry(i);
            if (i2 > 1) {
                return 0;
            }
            rtn *= 1 - i2;
        }
        return Math.pow(0.75, d) * rtn;
    }

    @Override
    public double norm() {
        return this.norm;
    }

    @Override
    public double norm1D() {
        return this.norm1D;
    }

    @Override
    public double secondMoment() {
        return this.secondMoment;
    }

    @Override
    public double secondMoment1D() {
        return this.secondMoment1D;
    }

    @Override
    public double effectTiveSupportWidth1D() {
        return 1.0;
    }
}

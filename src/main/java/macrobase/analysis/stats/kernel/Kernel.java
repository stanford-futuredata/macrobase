package macrobase.analysis.stats.kernel;

import org.apache.commons.math3.linear.RealVector;

public abstract class Kernel {
    public abstract double density(RealVector u);

    public abstract double norm();

    public abstract double norm1D();

    public abstract double secondMoment();

    public abstract double secondMoment1D();

    public abstract double effectTiveSupportWidth1D();
}

package macrobase.analysis.stats.kernel;

import org.apache.commons.math3.linear.RealVector;

public interface Kernel {
    double density(RealVector u);

    double norm();

    double norm1D();

    double secondMoment();

    double secondMoment1D();

    double effectiveSupportWidth1D();
}

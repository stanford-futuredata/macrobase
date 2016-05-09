package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.linear.RealVector;

public interface MultivariateDistribution {
    double density(RealVector vector);
}

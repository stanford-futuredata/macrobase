package macrobase.analysis.stats.distribution;

import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Mixture implements MultivariateDistribution {
    private static final Logger log = LoggerFactory.getLogger(Mixture.class);
    private List<MultivariateDistribution> components;
    private double[] weights;

    public Mixture(List<MultivariateDistribution> components, double[] weights) {
        this.components = components;
        this.weights = weights;
    }

    @Override
    public double density(RealVector vector) {
        double d = 0;
        for (int i=0; i< components.size(); i++) {
            d += components.get(i).density(vector) * weights[i];
        }
        return d;
    }
}

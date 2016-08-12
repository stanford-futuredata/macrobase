package macrobase.analysis.stats;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * BayesianNormalDensity fits a Gaussian distribution to a
 * multidimensional dataset, using Bayesian Inference and starting
 * with a simple Prior.
 * The predictive distribution for the density is a multivariate Student T distribution.
 */
public class BayesianNormalDensity extends BatchTrainScore {
    private static final Logger log = LoggerFactory.getLogger(BayesianNormalDensity.class);

    // prior covariance parameters for a Wishart distribution.
    private int priorDegreesOfFreedom;
    private RealMatrix priorCovariance;
    // prior mean parameters for Normal distribution. N(u, s * W(v, C))
    private RealVector priorMean;
    private double priorVarianceScale;

    // posterior covariance parameters for a Wishart distribution.
    private int posteriorDegreesOfFreedom;
    private RealMatrix posteriorCovariance;
    // posterior mean parameters for Normal distribution. N(u, s * W(v, C))
    private RealVector posteriorMean;
    private double posteriorVarianceScale;

    private MultivariateTDistribution distribution;

    public BayesianNormalDensity(MacroBaseConf conf) {
        super(conf);
    }

    @Override
    public void train(List<Datum> data) {
        System.out.println("training in ...");
        int dimensions = data.get(0).metrics().getDimension();
        priorMean = new ArrayRealVector(dimensions);
        priorVarianceScale = 1;
        priorDegreesOfFreedom = 1;
        priorCovariance = MatrixUtils.createRealIdentityMatrix(dimensions);
        int N = data.size();

        RealVector sampleSum = new ArrayRealVector(dimensions);
        for (Datum datum : data) {
            sampleSum = sampleSum.add(datum.metrics());
        }
        RealVector sampleMean = sampleSum.mapDivide(N);
        RealMatrix sampleCovarianceSum = new BlockRealMatrix(dimensions, dimensions);
        RealVector diff;
        for (Datum datum : data) {
            diff = datum.metrics().subtract(sampleMean);
            sampleCovarianceSum = sampleCovarianceSum.add(diff.outerProduct(diff));
        }
        posteriorVarianceScale = priorVarianceScale + N;
        posteriorMean = sampleSum.add(priorMean.mapMultiply(priorVarianceScale)).mapDivide(posteriorVarianceScale);
        posteriorDegreesOfFreedom = priorDegreesOfFreedom + N/2;
        RealVector _diff = sampleMean.subtract(priorMean);
        posteriorCovariance = priorCovariance.add(sampleCovarianceSum.scalarMultiply(.5))
                .add(_diff.outerProduct(_diff).scalarMultiply(N /2 * priorVarianceScale / posteriorVarianceScale));

        double varianceScaleForStudentT = (posteriorVarianceScale + 1) /
                posteriorVarianceScale /
                (posteriorDegreesOfFreedom + 1 - 0.5 * dimensions);
        distribution = new MultivariateTDistribution(posteriorMean,
                posteriorCovariance.scalarMultiply(varianceScaleForStudentT),
                posteriorDegreesOfFreedom - dimensions + 1);
    }

    public RealVector getMean() {
        return posteriorMean;
    }

    @Override
    public double score(Datum datum) {
        return 1. / getDensity(datum);
    }

    public double getDensity(Datum datum) {
        return distribution.density(datum.metrics());
    }
}

package macrobase.analysis.outlier;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.codahale.metrics.Counter;

import macrobase.MacroBase;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.datamodel.HasMetrics;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularMatrixException;
import org.apache.commons.math3.linear.SingularValueDecomposition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

public class MinCovDet extends OutlierDetector {
    private static final Logger log = LoggerFactory.getLogger(MinCovDet.class);

    private final Timer chooseKRandom = MacroBase.metrics.timer(name(MinCovDet.class, "chooseKRandom"));
    private final Timer meanComputation = MacroBase.metrics.timer(name(MinCovDet.class, "meanComputation"));
    private final Timer covarianceComputation = MacroBase.metrics.timer(name(MinCovDet.class, "covarianceComputation"));
    private final Timer determinantComputation = MacroBase.metrics.timer(
            name(MinCovDet.class, "determinantComputation"));
    private final Timer findKClosest = MacroBase.metrics.timer(name(MinCovDet.class, "findKClosest"));
    private final Counter singularCovariances = MacroBase.metrics.counter(name(MinCovDet.class, "singularCovariances"));

    public static class MetricsWithScore implements HasMetrics {
        private RealVector metrics;
        private Double score;

        public MetricsWithScore(RealVector metrics,
                                double score) {
            this.metrics = metrics;
            this.score = score;
        }

        public RealVector getMetrics() {
            return metrics;
        }

        public Double getScore() {
            return score;
        }
    }

    // p == dataset dimension
    private final int p;
    // H = alpha*(n+p+1)
    private double alpha;
    private Random random = new Random();
    private double stoppingDelta;

    // Local parameters used for training
    private RealMatrix localCov;
    private RealMatrix localInverseCov;
    private RealVector localMean;

    // Global parameters used for scoring
    private RealMatrix cov;
    private RealMatrix inverseCov;
    private RealVector mean;

    private int numSamples;

    public void seedRandom(long seed) {
        random = new Random(seed);
    }

    // efficient only when k << allData.size()
    private List<Datum> chooseKRandom(List<Datum> allData, final int k) {
        assert (k < allData.size());

        List<Datum> ret = new ArrayList<>();
        Set<Integer> alreadyChosen = new HashSet<>();
        while (ret.size() < k) {
            int idx = random.nextInt(allData.size());
            if (!alreadyChosen.contains(idx)) {
                alreadyChosen.add(idx);
                ret.add(allData.get(idx));
            }
        }

        assert (ret.size() == k);
        return ret;
    }

    public MinCovDet(MacroBaseConf conf) {
        super(conf);
        try {
            this.p = conf.getStringList(MacroBaseConf.LOW_METRICS).size() + conf.getStringList(MacroBaseConf.HIGH_METRICS).size();
        } catch (ConfigurationException e) {
            // Should never happen, but to avoid having to add throws
            // declaration, we re-throw as RuntimeException.
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        this.alpha = conf.getDouble(MacroBaseConf.MCD_ALPHA, MacroBaseDefaults.MCD_ALPHA);
        this.stoppingDelta = conf.getDouble(MacroBaseConf.MCD_STOPPING_DELTA, MacroBaseDefaults.MCD_STOPPING_DELTA);
    }

    public static double getMahalanobis(RealVector mean,
                                        RealMatrix inverseCov,
                                        RealVector vec) {
        final int dim = mean.getDimension();
        double[] vecMinusMean = new double[dim];

        for (int d = 0; d < dim; ++d) {
            vecMinusMean[d] = vec.getEntry(d) - mean.getEntry(d);
        }

        double diagSum = 0, nonDiagSum = 0;

        for (int d1 = 0; d1 < dim; ++d1) {
            for (int d2 = d1; d2 < dim; ++d2) {
                double v = vecMinusMean[d1] * vecMinusMean[d2] * inverseCov.getEntry(d1, d2);
                if (d1 == d2) {
                    diagSum += v;
                } else {
                    nonDiagSum += v;
                }
            }
        }

        return Math.sqrt(diagSum + 2 * nonDiagSum);
    }

    public RealVector getMean(List<? extends HasMetrics> data) {
        RealVector vec = null;

        for (HasMetrics d : data) {
            RealVector dvec = d.getMetrics();
            if (vec == null) {
                vec = dvec;
            } else {
                vec = vec.add(dvec);
            }
        }

        return vec.mapDivide(data.size());
    }

    private List<MetricsWithScore> findKClosest(int k, List<? extends HasMetrics> data) {
        List<MetricsWithScore> scores = new ArrayList<>();

        for (int i = 0; i < data.size(); ++i) {
            HasMetrics d = data.get(i);
            scores.add(new MetricsWithScore(d.getMetrics(),
                                            getMahalanobis(localMean, localInverseCov, d.getMetrics())));
        }

        if (scores.size() < k) {
            return scores;
        }

        scores.sort((a, b) -> a.getScore().compareTo(b.getScore()));

        return scores.subList(0, k);
    }

    // helper method
    public static double getDeterminant(RealMatrix cov) {
        return (new LUDecomposition(cov)).getDeterminant();
    }

    public static CovarianceMatrixAndMean
        combineCovarianceMatrices(List<RealMatrix> covarianceMatrices,
                              List<RealVector> means,
                              List<Double> allNumSamples) {
        RealMatrix covarianceMatrix = new Array2DRowRealMatrix(
                covarianceMatrices.get(0).getData()).scalarMultiply(allNumSamples.get(0));
        RealVector mean = new ArrayRealVector(means.get(0));
        double numSamples1 = allNumSamples.get(0);
        for (int j = 1; j < covarianceMatrices.size(); j++) {
            // Update covariance matrices and means
            // First update covariance matrices
            covarianceMatrix = covarianceMatrix.add(
                    covarianceMatrices.get(j).scalarMultiply(allNumSamples.get(j)));
            double numSamples2 = allNumSamples.get(j);
            RealVector mean2 = new ArrayRealVector(means.get(j));
            RealVector meanDifference = mean.subtract(mean2);
            covarianceMatrix = covarianceMatrix.add(
                    meanDifference.outerProduct(meanDifference).scalarMultiply((numSamples1 * numSamples2) /
                            (numSamples1 + numSamples2)));

            // Now update means
            mean.mapMultiplyToSelf(numSamples1);
            mean2.mapMultiplyToSelf(numSamples2);
            mean = mean.add(mean2);
            mean.mapDivideToSelf(numSamples1 + numSamples2);
            numSamples1 += numSamples2;
        }
        covarianceMatrix = covarianceMatrix.scalarMultiply(1.0 / numSamples1);

        return new CovarianceMatrixAndMean(mean, covarianceMatrix);
    }

    private void updateLocalInverseCovariance() {
        try {
            localInverseCov = new LUDecomposition(localCov).getSolver().getInverse();
        } catch (SingularMatrixException e) {
            singularCovariances.inc();
            localInverseCov = new SingularValueDecomposition(localCov).getSolver().getInverse();
        }
    }

    private void updateInverseCovariance() {
        try {
            inverseCov = new LUDecomposition(cov).getSolver().getInverse();
        } catch (SingularMatrixException e) {
            singularCovariances.inc();
            inverseCov = new SingularValueDecomposition(cov).getSolver().getInverse();
        }
    }

    @Override
    public void train(List<Datum> data) {
        // for now, only handle multivariate case...
        assert (data.iterator().next().getMetrics().getDimension() == p);
        assert (p > 1);

        int h = (int) Math.floor((data.size() + p + 1) * alpha);
        numSamples = h;

        // select initial dataset
        Timer.Context context = chooseKRandom.time();
        List<? extends HasMetrics> initialSubset = chooseKRandom(data, h);
        context.stop();

        context = meanComputation.time();
        localMean = getMean(initialSubset);
        context.stop();

        context = covarianceComputation.time();
        localCov = getCovariance(initialSubset);
        updateLocalInverseCovariance();
        context.stop();

        context = determinantComputation.time();
        double det = getDeterminant(localCov);
        context.stop();

        int stepNo = 1;

        // now take C-steps
        int numIterations = 1;
        while (true) {
            context = findKClosest.time();
            List<? extends HasMetrics> newH = findKClosest(h, data);
            context.stop();

            context = meanComputation.time();
            localMean = getMean(newH);
            context.stop();

            context = covarianceComputation.time();
            localCov = getCovariance(newH);
            updateLocalInverseCovariance();
            context.stop();

            context = determinantComputation.time();
            double newDet = getDeterminant(localCov);
            context.stop();

            double delta = det - newDet;

            if (newDet == 0 || delta < stoppingDelta) {
                break;
            }

            log.trace("Iteration {}: delta = {}; det = {}", stepNo, delta, newDet);
            det = newDet;
            stepNo++;

            numIterations++;
        }

        log.debug("Number of iterations in MCD step: {}", numIterations);

        log.trace("localMean: {}", localMean);
        log.trace("localCov: {}", localCov);
    }

    @Override
    public double score(Datum datum) {
        return getMahalanobis(mean, inverseCov, datum.getMetrics());
    }

    public RealMatrix getLocalCovariance() {
        return localCov;
    }
    public RealMatrix getLocalInverseCovariance() {
        return localInverseCov;
    }
    public RealVector getLocalMean() {
        return localMean;
    }

    public int getNumSamples() { return numSamples; }

    public void setCovariance(RealMatrix cov) {
        this.cov = cov;
        updateInverseCovariance();
    }
    public void setMean(RealVector mean) { this.mean = mean; }

    @Override
    public double getZScoreEquivalent(double zscore) {
        // compute zscore to CDF
        double cdf = (new NormalDistribution()).cumulativeProbability(zscore);
        // for normal distribution, mahalanobis distance is chi-squared
        // https://en.wikipedia.org/wiki/Mahalanobis_distance#Normal_distributions
        return (new ChiSquaredDistribution(p)).inverseCumulativeProbability(cdf);
    }

    @Override
    public ODDetectorType getODDetectorType() {
        return ODDetectorType.MCD;
    }
}

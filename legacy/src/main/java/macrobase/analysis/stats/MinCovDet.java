package macrobase.analysis.stats;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.codahale.metrics.Counter;

import macrobase.MacroBase;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularMatrixException;
import org.apache.commons.math3.linear.SingularValueDecomposition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

public class MinCovDet extends BatchTrainScore {
    private static final Logger log = LoggerFactory.getLogger(MinCovDet.class);

    private final Timer chooseKRandom = MacroBase.metrics.timer(name(MinCovDet.class, "chooseKRandom"));
    private final Timer meanComputation = MacroBase.metrics.timer(name(MinCovDet.class, "meanComputation"));
    private final Timer covarianceComputation = MacroBase.metrics.timer(name(MinCovDet.class, "covarianceComputation"));
    private final Timer determinantComputation = MacroBase.metrics.timer(
            name(MinCovDet.class, "determinantComputation"));
    private final Timer findKClosest = MacroBase.metrics.timer(name(MinCovDet.class, "findKClosest"));
    private final Counter singularCovariances = MacroBase.metrics.counter(name(MinCovDet.class, "singularCovariances"));

    // p == dataset dimension
    private final int p;
    // H = alpha*(n+p+1)
    private double alpha;
    private final Random random;
    private double stoppingDelta;

    private RealMatrix cov;
    private RealMatrix inverseCov;

    private RealVector mean;

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
            this.p = conf.getStringList(MacroBaseConf.METRICS).size();
        } catch (ConfigurationException e) {
            // Should never happen, but to avoid having to add throws
            // declaration, we re-throw as RuntimeException.
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        this.alpha = conf.getDouble(MacroBaseConf.MCD_ALPHA, MacroBaseDefaults.MCD_ALPHA);
        this.stoppingDelta = conf.getDouble(MacroBaseConf.MCD_STOPPING_DELTA, MacroBaseDefaults.MCD_STOPPING_DELTA);
        this.random = conf.getRandom();
    }

    public static Double getMahalanobis(RealVector mean,
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

    private RealVector getMean(List<Datum> data) {
        RealVector vec = null;

        for (Datum d : data) {
            RealVector dvec = d.metrics();
            if (vec == null) {
                vec = dvec;
            } else {
                vec = vec.add(dvec);
            }
        }

        return vec.mapDivide(data.size());
    }

    private List<Datum> findKClosest(int k, List<Datum> data) {
        if (data.size() < k) {
            return data;
        }

        Map<Datum, Double> scoreMap = new HashMap<>(data.size());
        for (Datum d : data) {
            scoreMap.put(d, getMahalanobis(mean, inverseCov, d.metrics()));
        }

        data.sort((a, b) -> scoreMap.get(a).compareTo(scoreMap.get(b)));

        return data.subList(0, k);
    }

    // helper method
    public static double getDeterminant(RealMatrix cov) {
        return (new LUDecomposition(cov)).getDeterminant();
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
    public void train(List<Datum> input) {
        // for now, only handle multivariate case...
        List<Datum> data = new ArrayList<>(input);
        assert (data.iterator().next().metrics().getDimension() == p);
        assert (p > 1);

        int h = (int) Math.floor((data.size() + p + 1) * alpha);

        // select initial dataset
        Timer.Context context = chooseKRandom.time();
        List<Datum> initialSubset = chooseKRandom(data, h);
        context.stop();

        context = meanComputation.time();
        mean = getMean(initialSubset);
        context.stop();

        context = covarianceComputation.time();
        cov = Covariance.getCovariance(initialSubset);
        updateInverseCovariance();
        context.stop();

        context = determinantComputation.time();
        double det = getDeterminant(cov);
        context.stop();

        int stepNo = 1;

        // now take C-steps
        int numIterations = 1;
        while (true) {
            context = findKClosest.time();
            List<Datum> newH = findKClosest(h, data);
            context.stop();

            context = meanComputation.time();
            mean = getMean(newH);
            context.stop();

            context = covarianceComputation.time();
            cov = Covariance.getCovariance(newH);
            updateInverseCovariance();
            context.stop();

            context = determinantComputation.time();
            double newDet = getDeterminant(cov);
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

        log.trace("mean: {}", mean);
        log.trace("cov: {}", cov);
    }

    @Override
    public double score(Datum datum) {
        return getMahalanobis(mean, inverseCov, datum.metrics());
    }

    public RealMatrix getCovariance() {
        return cov;
    }

    public RealMatrix getInverseCovariance() {
        return inverseCov;
    }

    public RealVector getMean() {
        return mean;
    }

    public double getZScoreEquivalent(double zscore) {
        // compute zscore to CDF
        double cdf = (new NormalDistribution()).cumulativeProbability(zscore);
        // for normal distribution, mahalanobis distance is chi-squared
        // https://en.wikipedia.org/wiki/Mahalanobis_distance#Normal_distributions
        return (new ChiSquaredDistribution(p)).inverseCumulativeProbability(cdf);
    }
}

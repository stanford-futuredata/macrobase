package macrobase.analysis.stats;

import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.descriptive.moment.VectorialCovariance;

import java.util.Arrays;
import java.util.List;

public class Gaussian {
    public int k=0;
    public double[] mean;
    public RealMatrix cov;

    public double[][] inverseCov;
    public boolean singular = false;

    public Gaussian() {}
    public Gaussian(double[] mean, RealMatrix cov) {
        this.k = mean.length;
        this.mean = Arrays.copyOf(mean, mean.length);
        this.cov = cov;
        initialize();
    }

    public Gaussian fit(List<double[]> data) {
        k = data.get(0).length;
        int n = data.size();
        VectorialCovariance covCounter = new VectorialCovariance(k, true);
        double[] sumCounter = new double[k];

        for(double[] curDatum : data) {
            for (int i = 0; i < k; i++) {
                sumCounter[i] += curDatum[i];
            }
            covCounter.increment(curDatum);
        }
        for (int i = 0; i < k; i++) {
            sumCounter[i] /= n;
        }
        mean = sumCounter;
        cov = covCounter.getResult();
        initialize();
        return this;
    }

    public void initialize() {
        try {
            inverseCov = new LUDecomposition(cov).getSolver().getInverse().getData();
        } catch (SingularMatrixException e) {
            singular = true;
            inverseCov = new SingularValueDecomposition(cov).getSolver().getInverse().getData();
        }
    }

    public double mahalanobis(double[] query) {
        double[] delta = new double[k];

        for (int i = 0; i < k; i++) {
            delta[i] = query[i] - mean[i];
        }

        double diagSum = 0, nonDiagSum = 0;

        for (int d1 = 0; d1 < k; ++d1) {
            for (int d2 = d1; d2 < k; ++d2) {
                double v = delta[d1] * delta[d2] * inverseCov[d1][d2];
                if (d1 == d2) {
                    diagSum += v;
                } else {
                    nonDiagSum += v;
                }
            }
        }

        return diagSum + 2 * nonDiagSum;
    }

    public double[] getMean() {
        return mean;
    }
    public RealMatrix getCovariance() {
        return cov;
    }
}

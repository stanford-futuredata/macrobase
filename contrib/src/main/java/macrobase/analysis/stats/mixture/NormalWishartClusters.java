package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.analysis.stats.distribution.Wishart;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * NormalWishartClusters is a class that represents K components (atoms) that
 * are Normal-Wishart distributed and have Normal-Wishart distribution each.
 * It also has methods for initializing these components (atoms) and
 * running Mean-Field Variational Inference and Stochastic Variation Inference.
 */
public class NormalWishartClusters {
    private static final Logger log = LoggerFactory.getLogger(NormalWishartClusters.class);

    // Omega and dof for Wishart distribution for the precision matrix of the clusters.
    private double dof[];
    private List<RealMatrix> omega;
    // Parameters for Normal distribution for atom position, N(loc, (beta * lambda))
    // where Lambda is Wishart distributed given parameters above.
    protected double beta[];
    protected List<RealVector> loc;

    // Base distribution, also needs to be Normal-Wishart
    private double baseNu;
    private RealMatrix baseOmega;
    private RealMatrix baseOmegaInverse;
    private double baseBeta;
    private RealVector baseLoc;

    private int K;
    private int D;
    private double halfDimensionLn2Pi;

    public NormalWishartClusters(int K, int dimension) {
        this.K = K;

        this.D = dimension;
        halfDimensionLn2Pi = 0.5 * D * Math.log(2 * Math.PI);
    }

    protected static List<RealMatrix> calculateQuadraticForms(List<Datum> data, List<RealVector> clusterMean, double[][] r) {
        int D = data.get(0).metrics().getDimension();
        int K = clusterMean.size();
        int N = data.size();
        List<RealMatrix> quadForm = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            RealMatrix form = new BlockRealMatrix(D, D);
            for (int n = 0; n < N; n++) {
                RealVector _diff = data.get(n).metrics().subtract(clusterMean.get(k));
                form = form.add(_diff.outerProduct(_diff).scalarMultiply(r[n][k]));
            }
            quadForm.add(form);
        }
        return quadForm;
    }

    protected static List<RealVector> calculateWeightedSums(List<Datum> data, double[][] r) {
        int N = data.size();
        int K = r[0].length;
        int D = data.get(0).metrics().getDimension();
        List<RealVector> sums = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            RealVector sum = new ArrayRealVector(D);
            for (int n = 0; n < N; n++) {
                sum = sum.add(data.get(n).metrics().mapMultiply(r[n][k]));
            }
            sums.add(sum);
        }
        return sums;
    }

    /**
     * Initializes base distribution. This method works great with DP mixture model.
     * @param data
     */
    public void initializeBaseForDP(List<Datum> data) {
        int dimension = data.get(0).metrics().getDimension();
        baseNu = dimension;
        double[][] boundingBox = AlgebraUtils.getBoundingBox(data);
        double[] midpoints = new double[dimension];
        double[] dimensionWidth = new double[dimension];
        double R = 0;  // value of the widest dimension.
        for (int i = 0; i < dimension; i++) {
            dimensionWidth[i] = boundingBox[i][1] - boundingBox[i][0];
            midpoints[i] = boundingBox[i][0] + dimensionWidth[i];
            if (dimensionWidth[i] > R) {
                R = dimensionWidth[i];
            }
        }
        baseBeta = Math.pow(R, -2);
        baseLoc = new ArrayRealVector(midpoints);
        baseOmegaInverse = MatrixUtils.createRealIdentityMatrix(dimension);
    }

    /**
     * Initializes atom (component) distributions. This method works great with DP mixture model.
     * @param data
     */
    public void initializeAtomsForDP(List<Datum> data, String filename, Random random) {
        omega = new ArrayList<>(K);
        dof = new double[K];
        beta = new double[K];

        // if filename was provided, initialize K atoms (points) from file,
        // if the file contains less than K points, take those points as seeds.
        if (filename != null) {
            try {
                loc = BatchMixtureModel.initializeClustersFromFile(filename, K);
                log.debug("loc : {}", loc);
                if(loc.size() < K) {
                    loc = BatchMixtureModel.gonzalezInitializeMixtureCenters(loc, data, K, random);
                }
            } catch (FileNotFoundException e) {
                log.debug("failed to initialized from file");
                e.printStackTrace();
                loc = BatchMixtureModel.gonzalezInitializeMixtureCenters(data, K, random);
            }
        } else {
            loc = BatchMixtureModel.gonzalezInitializeMixtureCenters(data, K, random);
        }
        for (int i = 0; i < K; i++) {
            // initialize betas as if all points are from the first cluster.
            beta[i] = 1;
            dof[i] = baseNu;
            omega.add(0, AlgebraUtils.invertMatrix(baseOmegaInverse));
        }
    }

    /**
     * Initializes atom (component) distributions. This method works great with finite mixture model.
     * @param data
     */
    public void initializeAtomsForFinite(List<Datum> data, String filename, Random random) {

        beta = new double[K];
        dof = new double[K];
        omega = new ArrayList<>(K);

        // if filename was provided, initialize K atoms (points) from file,
        // if the file contains less than K points, take those points as seeds.
        if (filename != null) {
            try {
                loc = BatchMixtureModel.initializeClustersFromFile(filename, K);
                if(loc.size() < K) {
                    loc = BatchMixtureModel.gonzalezInitializeMixtureCenters(loc, data, K, random);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                loc = BatchMixtureModel.gonzalezInitializeMixtureCenters(data, K, random);
            }
        } else {
            loc = BatchMixtureModel.gonzalezInitializeMixtureCenters(data, K, random);
        }
        log.debug("initialized cluster centers as: {}", loc);
        for (int k = 0; k < this.K; k++) {
            beta[k] = baseBeta;
            dof[k] = baseNu;
            omega.add(baseOmega);
        }
    }

    public void initializeBaseForFinite(List<Datum> data) {
        baseNu = 0.1;
        baseBeta = 0.1;
        baseLoc = new ArrayRealVector(D);
        baseOmega = MatrixUtils.createRealIdentityMatrix(D);
        baseOmegaInverse = AlgebraUtils.invertMatrix(baseOmega);
    }

    public double[] calculateExLogPrecision() {
        double[] lnPrecision = new double[K];
        for (int i = 0; i < K; i++) {
            lnPrecision[i] = 0.5 * (new Wishart(omega.get(i), dof[i])).getExpectationLogDeterminantLambda();
        }
        return lnPrecision;
    }

    public double[][] calcLogLikelyFixedPrec(List<Datum> data) {
        int N = data.size();
        double[][] loglike = new double[N][K];
        for (int k = 0; k < K; k++) {
            for (int n = 0; n < N; n++) {
                RealVector _diff = data.get(n).metrics().subtract(loc.get(k));
                loglike[n][k] = -halfDimensionLn2Pi - 0.5 * (
                        D / beta[k] + dof[k] * _diff.dotProduct(omega.get(k).operate(_diff)));
            }
        }
        return loglike;
    }

    public void update(List<Datum> data, double[][] r) {
        double[] clusterWeight = VariationalInference.calculateClusterWeights(r);
        List<RealVector> weightedSum = calculateWeightedSums(data, r);
        List<RealVector> clusterMean = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            if (clusterWeight[k] > 0) {
                clusterMean.add(weightedSum.get(k).mapDivide(clusterWeight[k]));
            } else {
                clusterMean.add(weightedSum.get(k));
            }
        }
        List<RealMatrix> quadForm = calculateQuadraticForms(data, clusterMean, r);

        for (int k = 0; k < K; k++) {
            beta[k] = baseBeta + clusterWeight[k];
            loc.set(k, baseLoc.mapMultiply(baseBeta).add(weightedSum.get(k)).mapDivide(beta[k]));
            dof[k] = baseNu + 1 + clusterWeight[k];
            RealVector adjustedMean = clusterMean.get(k).subtract(baseLoc);
            RealMatrix wInverse = baseOmegaInverse
                    .add(quadForm.get(k))
                    .add(adjustedMean.outerProduct(adjustedMean).scalarMultiply(baseBeta * clusterWeight[k] / (baseBeta + clusterWeight[k])));
            omega.set(k, AlgebraUtils.invertMatrix(wInverse));
        }
        log.debug("clusterWeights: {}", clusterWeight);
    }

    public void moveNatural(List<Datum> data, double[][] r, double pace, double repeat) {
        double[] clusterWeight = VariationalInference.calculateClusterWeights(r);
        List<RealVector> weightedSum = calculateWeightedSums(data, r);
        List<RealVector> clusterMean = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            if (clusterWeight[k] > 0) {
                clusterMean.add(weightedSum.get(k).mapDivide(clusterWeight[k]));
            } else {
                clusterMean.add(weightedSum.get(k));
            }
            // Multiply by repeat to get actual numbers
            clusterWeight[k] *= repeat;
            weightedSum.set(k, weightedSum.get(k).mapMultiply(repeat));
        }
        List<RealMatrix> quadForm = calculateQuadraticForms(data, clusterMean, r);
        for (int i = 0; i< quadForm.size(); i++) {
            quadForm.set(i, quadForm.get(i).scalarMultiply(repeat));
        }

        for (int k = 0; k < K; k++) {
            beta[k] = VariationalInference.step(beta[k], baseBeta + clusterWeight[k], pace);
            loc.set(k, VariationalInference.step(loc.get(k), baseLoc.mapMultiply(baseBeta).add(weightedSum.get(k)).mapDivide(beta[k]), pace));
            dof[k] = VariationalInference.step(dof[k], baseNu + 1 + clusterWeight[k], pace);
            RealVector adjustedMean = clusterMean.get(k).subtract(baseLoc);
            RealMatrix wInverse = baseOmegaInverse
                    .add(quadForm.get(k))
                    .add(adjustedMean.outerProduct(adjustedMean).scalarMultiply(baseBeta * clusterWeight[k] / (baseBeta + clusterWeight[k])));
            omega.set(k, VariationalInference.step(omega.get(k), AlgebraUtils.invertMatrix(wInverse), pace));
        }
    }

    public List<MultivariateTDistribution> constructPredictiveDistributions() {
        List<MultivariateTDistribution> predictiveDistributions = new ArrayList<>(K);
        for (int k = 0; k < this.K; k++) {
            double scale = (dof[k] + 1 - D) * beta[k] / (1 + beta[k]);
            RealMatrix ll = AlgebraUtils.invertMatrix(omega.get(k).scalarMultiply(scale));
            predictiveDistributions.add(new MultivariateTDistribution(loc.get(k), ll, dof[k] + 1 - D));
        }
        return predictiveDistributions;
    }

    public List<RealMatrix> getMAPCovariances() {
        List<RealMatrix> covariances = new ArrayList<>(omega.size());
        for (int i = 0; i < omega.size(); i++) {
            double scale = (dof[i] + 1 - D) * beta[i] / (1 + beta[i]);
            covariances.add(AlgebraUtils.invertMatrix(omega.get(i).scalarMultiply(scale)));
        }
        return covariances;
    }

    public List<RealVector> getMAPLocations() {
        return loc;
    }
}

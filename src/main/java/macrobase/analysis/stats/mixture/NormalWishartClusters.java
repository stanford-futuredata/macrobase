package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.analysis.stats.distribution.Wishart;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

    public void initalizeAtomsForFinite(List<Datum> data, String filename, Random random) {

        beta = new double[K];
        dof = new double[K];
        omega = new ArrayList<>(K);

        // Initialize
        if (filename != null) {
            try {
                loc = BatchMixtureModel.initalizeClustersFromFile(filename, K);
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
        log.debug("atomOmega: {}", omega);
    }

    public void initializeBaseForFinite(List<Datum> data) {
        baseNu = 0.1;
        baseBeta = 0.1;
        baseLoc = new ArrayRealVector(D);
        baseOmega = MatrixUtils.createRealIdentityMatrix(D);
        baseOmegaInverse = AlgebraUtils.invertMatrix(baseOmega);
    }

    public void initializeBase(RealVector loc, double beta, RealMatrix omega, double dof) {
        baseLoc = loc;
        baseBeta = beta;
        baseOmega = omega;
        baseOmegaInverse = AlgebraUtils.invertMatrix(baseOmega);
        baseNu = dof;
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
                RealVector _diff = data.get(n).getMetrics().subtract(loc.get(k));
                loglike[n][k] = -halfDimensionLn2Pi - 0.5 * (
                        D / beta[k] + dof[k] * _diff.dotProduct(omega.get(k).operate(_diff)));
            }
        }
        return loglike;
    }

    public void update(List<Datum> data, double[][] r) {
        double[] clusterWeight = MeanFieldGMM.calculateClusterWeights(r);
        List<RealVector> weightedSum = MeanFieldGMM.calculateWeightedSums(data, r);
        List<RealVector> clusterMean = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            if (clusterWeight[k] > 0) {
                clusterMean.add(weightedSum.get(k).mapDivide(clusterWeight[k]));
            } else {
                clusterMean.add(weightedSum.get(k));
                log.debug("weighted sum = {} (should be 0)", weightedSum.get(k));
            }
        }
        List<RealMatrix> quadForm = MeanFieldGMM.calculateQuadraticForms(data, clusterMean, r);
        log.debug("clusterWeights: {}", clusterWeight);

        for (int k = 0; k < K; k++) {
            beta[k] = baseBeta + clusterWeight[k];
            loc.set(k, baseLoc.mapMultiply(baseBeta).add(weightedSum.get(k)).mapDivide(beta[k]));
            dof[k] = baseNu + 1 + clusterWeight[k];
            RealVector adjustedMean = clusterMean.get(k).subtract(baseLoc);
            RealMatrix wInverse = baseOmegaInverse
                    .add(quadForm.get(k))
                    .add(adjustedMean.outerProduct(adjustedMean).scalarMultiply(baseBeta * clusterWeight[k] / (baseBeta + clusterWeight[k])));
            log.debug("wInverse: {}", wInverse);
            omega.set(k, AlgebraUtils.invertMatrix(wInverse));
        }
    }

    public void moveNatural(List<Datum> data, double[][] r, double pace, double repeat) {
        double[] clusterWeight = MeanFieldGMM.calculateClusterWeights(r);
        List<RealVector> weightedSum = MeanFieldGMM.calculateWeightedSums(data, r);
        List<RealVector> clusterMean = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            if (clusterWeight[k] > 0) {
                clusterMean.add(weightedSum.get(k).mapDivide(clusterWeight[k]));
            } else {
                clusterMean.add(weightedSum.get(k));
                log.debug("weighted sum = {} (should be 0)", weightedSum.get(k));
            }
            // Multiply by repeat to get actual numbers
            clusterWeight[k] *= repeat;
            weightedSum.set(k, weightedSum.get(k).mapMultiply(repeat));
        }
        List<RealMatrix> quadForm = MeanFieldGMM.calculateQuadraticForms(data, clusterMean, r);
        log.debug("clusterWeights: {}", clusterWeight);

        for (int k = 0; k < K; k++) {
            beta[k] = StochVarInfGMM.step(beta[k], baseBeta + clusterWeight[k], pace);
            loc.set(k, StochVarInfGMM.step(loc.get(k), baseLoc.mapMultiply(baseBeta).add(weightedSum.get(k)).mapDivide(beta[k]), pace));
            dof[k] = StochVarInfGMM.step(dof[k], baseNu + 1 + clusterWeight[k], pace);
            RealVector adjustedMean = clusterMean.get(k).subtract(baseLoc);
            RealMatrix wInverse = baseOmegaInverse
                    .add(quadForm.get(k))
                    .add(adjustedMean.outerProduct(adjustedMean).scalarMultiply(baseBeta * clusterWeight[k] / (baseBeta + clusterWeight[k])));
            omega.set(k, StochVarInfGMM.step(omega.get(k), AlgebraUtils.invertMatrix(wInverse), pace));
        }
    }

    public List<MultivariateTDistribution> constructPredictiveDistributions() {
        List<MultivariateTDistribution> predictiveDistributions = new ArrayList<>(K);
        for (int k = 0; k < this.K; k++) {
            double scale = (dof[k] + 1 - D) * beta[k] / (1 + beta[k]);
            RealMatrix ll = AlgebraUtils.invertMatrix(omega.get(k).scalarMultiply(scale));
            // TODO: MultivariateTDistribution should support real values for 3rd parameters
            predictiveDistributions.add(new MultivariateTDistribution(loc.get(k), ll, (int) (dof[k] - 1 - D)));
        }
        return predictiveDistributions;
    }

    public List<RealMatrix> getMAPCovariances() {
        List<RealMatrix> covariances = new ArrayList<>(omega.size());
        for (int i = 0; i < omega.size(); i++) {
            covariances.add(AlgebraUtils.invertMatrix(omega.get(i).scalarMultiply(dof[i])));
        }
        return covariances;
    }

    public List<RealVector> getMAPLocations() {
        return loc;
    }
}

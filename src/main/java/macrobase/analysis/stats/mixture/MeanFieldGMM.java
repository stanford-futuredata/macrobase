package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.Wishart;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class MeanFieldGMM extends BatchMixtureModel {
    private static final Logger log = LoggerFactory.getLogger(MeanFieldGMM.class);
    protected double progressCutoff;

    // Parameters for Base Distribution, which is Wishart-Gaussian
    protected double baseNu;
    protected RealMatrix baseOmega;
    protected RealMatrix baseOmegaInverse;  // Use inverse of baseOmega, since it is used in the update equations.
    protected double baseBeta;
    protected RealVector baseLoc;

    // Variables governing atoms (components).
    // Omega and atomDOF for Wishart distribution for the precision matrix of the clusters.
    protected double atomDOF[];
    protected List<RealMatrix> atomOmega;
    // Parameters for Normal distribution for atom position, N(atomLocation, (atomBeta * Lambda))
    // where Lambda is Wishart distributed given parameters above.
    protected double atomBeta[];
    protected List<RealVector> atomLoc;

    // Useful constants
    protected int D;
    protected double halfDimensionLn2Pi;
    protected double dimensionLn2;

    // Mean-Field iterative algorithm maximum iterations.
    protected final int maxIterationsToConverge;

    public MeanFieldGMM(MacroBaseConf conf) {
        super(conf);
        this.progressCutoff = conf.getDouble(MacroBaseConf.EM_CUTOFF_PROGRESS, MacroBaseDefaults.EM_CUTOFF_PROGRESS);
        maxIterationsToConverge = conf.getInt(MacroBaseConf.MIXTURE_MAX_ITERATIONS_TO_CONVERGE, MacroBaseDefaults.MIXTURE_MAX_ITERATIONS_TO_CONVERGE);
    }

    protected void initConstants(List<Datum> data) {
        D = data.get(0).getMetrics().getDimension();
        halfDimensionLn2Pi = 0.5 * D * Math.log(2 * Math.PI);
        dimensionLn2 = D * Math.log(2);
    }

    protected void initializeBaseNormalWishart(List<Datum> data) {
        baseNu = D;
        double[][] boundingBox = AlgebraUtils.getBoundingBox(data);
        double[] midpoints = new double[D];
        double[] dimensionWidth = new double[D];
        double R = 0;  // value of the widest dimension.
        for (int i = 0; i < D; i++) {
            dimensionWidth[i] = boundingBox[i][1] - boundingBox[i][0];
            midpoints[i] = boundingBox[i][0] + dimensionWidth[i];
            if (dimensionWidth[i] > R) {
                R = dimensionWidth[i];
            }
        }
        baseBeta = Math.pow(R, -1);
        baseLoc = new ArrayRealVector(midpoints);
        baseOmega = MatrixUtils.createRealIdentityMatrix(D).scalarMultiply(Math.pow(R, -2));
        baseOmegaInverse = AlgebraUtils.invertMatrix(baseOmega);
    }

    protected static List<RealMatrix> calculateQuadraticForms(List<Datum> data, List<RealVector> clusterMean, double[][] r) {
        int D = data.get(0).getMetrics().getDimension();
        int K = clusterMean.size();
        int N = data.size();
        List<RealMatrix> quadForm = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            RealMatrix form = new BlockRealMatrix(D, D);
            for (int n = 0; n < N; n++) {
                RealVector _diff = data.get(n).getMetrics().subtract(clusterMean.get(k));
                form = form.add(_diff.outerProduct(_diff).scalarMultiply(r[n][k]));
            }
            quadForm.add(form);
        }
        return quadForm;
    }

    protected static List<Wishart> constructWisharts(List<RealMatrix> omega, double[] dof) {
        int num = omega.size();
        List<Wishart> wisharts = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            wisharts.add(new Wishart(omega.get(i), dof[i]));
        }
        return wisharts;
    }

    protected List<RealVector> calculateWeightedSums(List<Datum> data, double[][] r) {
        int N = data.size();
        int K = r[0].length;
        List<RealVector> sums = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            RealVector sum = new ArrayRealVector(D);
            for (int n = 0; n < N; n++) {
                sum = sum.add(data.get(n).getMetrics().mapMultiply(r[n][k]));
            }
            sums.add(sum);
        }
        return sums;
    }

    protected double[] calculateClusterWeights(double[][] r) {
        int N = r.length;
        int K = r[0].length;
        double[] clusterWeight = new double[K];
        for (int k = 0; k < K; k++) {
            for (int n = 0; n < N; n++) {
                clusterWeight[k] += r[n][k];
            }
        }
        return clusterWeight;
    }

    protected double[][] calcLogLikelihoodFixedAtoms(List<Datum> data, List<RealVector> atomLoc, double[] atomBeta, List<RealMatrix> atomOmega, double[] atomDOF) {
        int N = data.size();
        int K = atomLoc.size();
        double[][] loglike = new double[N][K];
        for (int k = 0; k < K; k++) {
            for (int n = 0; n < N; n++) {
                RealVector _diff = data.get(n).getMetrics().subtract(atomLoc.get(k));
                loglike[n][k] = -halfDimensionLn2Pi - 0.5 * (
                        D / atomBeta[k] + atomDOF[k] * _diff.dotProduct(atomOmega.get(k).operate(_diff)));
            }
        }
        return loglike;
    }

    protected void updateAtoms(double[][] r, List<Datum> data) {
        double[] clusterWeight = calculateClusterWeights(r);
        int K = atomLoc.size();
        List<RealVector> weightedSum = calculateWeightedSums(data, r);
        List<RealVector> clusterMean = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            if (clusterWeight[k] > 0) {
                clusterMean.add(weightedSum.get(k).mapDivide(clusterWeight[k]));
            } else {
                clusterMean.add(weightedSum.get(k));
                log.debug("weighted sum = {} (should be 0)", weightedSum.get(k));
            }
        }
        List<RealMatrix> quadForm = calculateQuadraticForms(data, clusterMean, r);
        log.debug("clusterWeights: {}", clusterWeight);

        for (int k = 0; k < K; k++) {
            atomBeta[k] = baseBeta + clusterWeight[k];
            atomLoc.set(k, baseLoc.mapMultiply(baseBeta).add(weightedSum.get(k)).mapDivide(atomBeta[k]));
            atomDOF[k] = baseNu + 1 + clusterWeight[k];
            RealVector adjustedMean = clusterMean.get(k).subtract(baseLoc);
            //log.debug("adjustedMean: {}", adjustedMean);
            RealMatrix wInverse = baseOmegaInverse
                    .add(quadForm.get(k))
                    .add(adjustedMean.outerProduct(adjustedMean).scalarMultiply(baseBeta * clusterWeight[k] / (baseBeta + clusterWeight[k])));
            //log.debug("wInverse: {}", wInverse);
            atomOmega.set(k, AlgebraUtils.invertMatrix(wInverse));
        }
    }

}


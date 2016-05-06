package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.special.Gamma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Variational Dirichlet Process Mixture of Gaussians.
 */
public class VariationalDPGMM extends MeanFieldGMM {
    private static final Logger log = LoggerFactory.getLogger(VariationalDPGMM.class);

    // Number of truncated clusters.
    private int T;
    // Concentration parameter for the Dirichlet distribution.
    private double concentrationParameter;

    // Parameters describing stick lengths, i.e. shape parameters of Beta distributions.
    private double shapeParams[][];

    private List<MultivariateTDistribution> predictiveDistributions;

    @Override
    protected void updatePredictiveDistributions() {
        int dimension = atomLoc.get(0).getDimension();

        predictiveDistributions = new ArrayList<>(T);
        for (int t = 0; t < T; t++) {
            double scale = (atomDOF[t] + 1 - dimension) * atomBeta[t] / (1 + atomBeta[t]);
            RealMatrix ll = AlgebraUtils.invertMatrix(atomOmega.get(t).scalarMultiply(scale));
            // TODO: MultivariateTDistribution should support real values for 3rd parameters
            predictiveDistributions.add(new MultivariateTDistribution(atomLoc.get(t), ll, (int) (atomDOF[t] + 1 - dimension)));
        }
    }

    public VariationalDPGMM(MacroBaseConf conf) {
        super(conf);
        T = conf.getInt(MacroBaseConf.DPM_TRUNCATING_PARAMETER, MacroBaseDefaults.DPM_TRUNCATING_PARAMETER);
        concentrationParameter = conf.getDouble(MacroBaseConf.DPM_CONCENTRATION_PARAMETER, MacroBaseDefaults.DPM_CONCENTRATION_PARAMETER);
    }

    @Override
    protected void initializeBaseNormalWishart(List<Datum> data) {
        int dimension = data.get(0).getMetrics().getDimension();
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


    @Override
    protected void initializeAtoms(List<Datum> data) {
        atomOmega = new ArrayList<>(T);
        atomDOF = new double[T];
        atomBeta = new double[T];

        // atoms
        atomLoc = gonzalezInitializeMixtureCenters(data, T);
        for (int i = 0; i < T; i++) {
            // initialize betas as if all points are from the first cluster.
            atomBeta[i] = 1;
            atomDOF[i] = baseNu;
            atomOmega.add(0, AlgebraUtils.invertMatrix(baseOmegaInverse));
        }
    }

    @Override
    protected void initializeBaseMixing() {
        // concentrationParameter has been set in the constructor.
    }

    @Override
    protected void initializeSticks() {
        // stick lengths
        shapeParams = new double[T][2];
        for (int i = 0; i < T; i++) {
            shapeParams[i][0] = 1;
            shapeParams[i][1] = concentrationParameter;
        }
    }

    @Override
    protected double[] calcExQlogMixing() {
        double[] lnMixingContribution = new double[T];
        double cumulativeAlreadyAssigned = 0;
        for (int t = 0; t < T; t++) {
            // Calculate Mixing coefficient contributions to r
            lnMixingContribution[t] = cumulativeAlreadyAssigned + (Gamma.digamma(shapeParams[t][0]) - Gamma.digamma(shapeParams[t][0] + shapeParams[t][1]));
            cumulativeAlreadyAssigned += Gamma.digamma(shapeParams[t][1]) - Gamma.digamma(shapeParams[t][0] + shapeParams[t][1]);
        }
        return lnMixingContribution;
    }

    @Override
    public void train(List<Datum> data) {
        super.train(data, T);
    }

    @Override
    protected void updateSticks(double[][] r) {
        int N = r.length;
        for (int t = 0; t < atomLoc.size(); t++) {
            shapeParams[t][0] = 1;
            shapeParams[t][1] = concentrationParameter;
            for (int n = 0; n < N; n++) {
                shapeParams[t][0] += r[n][t];
                for (int j = t + 1; j < T; j++) {
                    shapeParams[t][1] += r[n][j];
                }
            }
        }
    }

    @Override
    public double score(Datum datum) {
        double density = 0;
        double[] stickLengths = getClusterProportions();
        for (int i = 0; i < predictiveDistributions.size(); i++) {
            density += stickLengths[i] * predictiveDistributions.get(i).density(datum.getMetrics());
        }
        return Math.log(density);
    }

    @Override
    public double[] getClusterProportions() {
        double[] proportions = new double[T];
        double stickRemaining = 1;
        double expectedBreak;
        for (int i = 0; i < T; i++) {
            expectedBreak = stickRemaining / (1 + shapeParams[i][1] / shapeParams[i][0]);
            stickRemaining -= expectedBreak;
            proportions[i] = expectedBreak;
        }
        return proportions;
    }


    @Override
    public double[] getClusterProbabilities(Datum d) {
        double[] probas = new double[T];
        double[] weights = getClusterProportions();
        double normalizingConstant = 0;
        for (int i = 0; i < T; i++) {
            probas[i] = weights[i] * predictiveDistributions.get(i).density(d.getMetrics());
            normalizingConstant += probas[i];
        }
        for (int i = 0; i < T; i++) {
            probas[i] /= normalizingConstant;
        }
        return probas;
    }
}

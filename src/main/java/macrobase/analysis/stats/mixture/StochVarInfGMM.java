package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.Wishart;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class StochVarInfGMM extends MeanFieldGMM {
    private static final Logger log = LoggerFactory.getLogger(StochVarInfGMM.class);
    private final int desiredMinibatchSize;
    private final double forgettingRate;
    private final double delay;

    public StochVarInfGMM(MacroBaseConf conf) {
        super(conf);
        desiredMinibatchSize = 500;//conf.getInt(MacroBaseConf.MINIBATCH_SIZE, MacroBaseDefaults.MINIBATCH_SIZE);
        forgettingRate = 0.7; //kappa from SVI paper
        //FIXME: check paper for good number for dealy!
        delay = 1; // tau from SVI paper
    }

    protected abstract void updateSticks(double[][] r, double stepSize, double repeat);

    public static double step(double value, double newValue, double pace) {
        return (1 - pace) * value + pace * newValue;
    }

    public static RealVector step(RealVector start, RealVector end, double pace) {
        return start.mapMultiply(1 - pace).add(end.mapMultiply(pace));
    }

    public static RealMatrix step(RealMatrix start, RealMatrix end, double pace) {
        return start.scalarMultiply(1 - pace).add(end.scalarMultiply(pace));
    }

    protected void updateAtoms(double[][] r, List<Datum> data, double stepSize, double repeat) {
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
            // Multiply by repeat to get actual numbers
            clusterWeight[k] *= repeat;
            weightedSum.set(k, weightedSum.get(k).mapMultiply(repeat));
        }
        List<RealMatrix> quadForm = calculateQuadraticForms(data, clusterMean, r);
        log.debug("clusterWeights: {}", clusterWeight);

        for (int k = 0; k < K; k++) {
            atomBeta[k] = step(atomBeta[k], baseBeta + clusterWeight[k], stepSize);
            atomLoc.set(k, step(atomLoc.get(k), baseLoc.mapMultiply(baseBeta).add(weightedSum.get(k)).mapDivide(atomBeta[k]), stepSize));
            atomDOF[k] = step(atomDOF[k], baseNu + 1 + clusterWeight[k], stepSize);
            RealVector adjustedMean = clusterMean.get(k).subtract(baseLoc);
            //log.debug("adjustedMean: {}", adjustedMean);
            RealMatrix wInverse = baseOmegaInverse
                    .add(quadForm.get(k))
                    .add(adjustedMean.outerProduct(adjustedMean).scalarMultiply(baseBeta * clusterWeight[k] / (baseBeta + clusterWeight[k])));
            //log.debug("wInverse: {}", wInverse);
            atomOmega.set(k, step(atomOmega.get(k), AlgebraUtils.invertMatrix(wInverse), stepSize));
        }
    }


    /**
     * @param data - data to train on
     * @param K    - run inference with K clusters
     */
    public void trainSVI(List<Datum> data, int K) {
        int N = data.size();
        // 0. Initialize all approximating factors
        initConstants(data);
        initializeBaseNormalWishart(data);
        initializeBaseMixing();
        initializeSticks();
        initializeAtoms(data);

        int partitions = N / desiredMinibatchSize;
        int minibatchSize;

        List<Wishart> wisharts;
        // density of each point with respect to each mixture component.
        double[][] r;

        double logLikelihood = -Double.MAX_VALUE;
        for (int iteration = 1; ; iteration++) {

            double stepSize = Math.pow(iteration + delay, -forgettingRate);

            for (int p = 0; p < partitions; p++) {

                // Useful to keep everything tidy.

                // 1. calculate expectation of densities of each point coming from individual clusters - r[n][k]
                // 1. Reevaluate r[][]

                // Create the minibatch.
                List<Datum> miniBatch = new ArrayList<>(desiredMinibatchSize);
                for (int i = 0; i < N; i += partitions) {
                    miniBatch.add(data.get(i));
                }
                minibatchSize = miniBatch.size();

                r = new double[minibatchSize][K];

                // Calculate mixing coefficient log expectation
                double[] exLnMixingContribution = calcExQlogMixing();
                double[] lnPrecision = calculateExLogPrecision(atomOmega, atomDOF);
                double[][] dataLogLike = calcLogLikelihoodFixedAtoms(miniBatch, atomLoc, atomBeta, atomOmega, atomDOF);
                r = normalizeLogProbas(exLnMixingContribution, lnPrecision, dataLogLike);

                // 2. Reevaluate clusters based on densities that we have for each point.
                // 2. Reevaluate atoms and stick lengths.

                updateSticks(r, stepSize, 1. * N / minibatchSize);
                updateAtoms(r, data, stepSize, 1. * N / minibatchSize);

            }

            updatePredictiveDistributions();

            double oldLogLikelihood = logLikelihood;
            logLikelihood = 0;
            for (int n = 0; n < N; n++) {
                logLikelihood += score(data.get(n));
            }
            log.debug("log likelihood after iteration {} is {}", iteration, logLikelihood);

            if (iteration >= maxIterationsToConverge) {
                log.debug("Breaking because have already run {} iterations", iteration);
                break;
            }

            double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
            if (improvement >= 0 && improvement < this.progressCutoff) {
                log.debug("Breaking because improvement was {} percent", improvement * 100);
                break;
            } else {
                log.debug("improvement is : {}%", improvement * 100);
            }
            log.debug(".........................................");
        }
    }


}

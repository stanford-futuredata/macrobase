package macrobase.analysis.stats.mixture;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public abstract class BatchMixtureModel extends BatchTrainScore {
    private static final Logger log = LoggerFactory.getLogger(BatchMixtureModel.class);
    protected final double progressCutoff;
    protected final int maxIterationsToConverge;
    protected MacroBaseConf conf;

    public BatchMixtureModel(MacroBaseConf conf) {
        super(conf);
        this.conf = conf;
        progressCutoff = conf.getDouble(MacroBaseConf.EM_CUTOFF_PROGRESS, MacroBaseDefaults.EM_CUTOFF_PROGRESS);
        maxIterationsToConverge = conf.getInt(MacroBaseConf.MIXTURE_MAX_ITERATIONS_TO_CONVERGE, MacroBaseDefaults.MIXTURE_MAX_ITERATIONS_TO_CONVERGE);
        log.debug("max iter = {}", maxIterationsToConverge);
    }

    public static List<RealVector> initializeClustersFromFile(String filename, int K) throws FileNotFoundException {
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(filename));
        RealVector[] centers = gson.fromJson(reader, ArrayRealVector[].class);
        List<RealVector> vectors = Arrays.asList(centers);
        return vectors.subList(0, K);
    }

    protected static List<RealVector> gonzalezInitializeMixtureCenters(List<Datum> data, int K, Random rand) {
        List<RealVector> vectors = new ArrayList<>(K);
        int N = data.size();
        HashSet<Integer> pointsChosen = new HashSet<Integer>();
        int index = rand.nextInt(data.size());
        for (int k = 0; k < K; k++) {
            if (k > 0) {
                double maxDistance = 0;
                for (int n = 0; n < N; n++) {
                    if (pointsChosen.contains(n)) {
                        continue;
                    }
                    double distance = 0;
                    for (int j = 0; j < k; j++) {
                        distance += data.get(n).getMetrics().getDistance(vectors.get(j));
                    }
                    if (distance > maxDistance) {
                        maxDistance = distance;
                        index = n;
                    }
                }
            }
            vectors.add(data.get(index).getMetrics());
            pointsChosen.add(index);
        }
        return vectors;
    }

    /**
     * @return centers of mixtures
     */
    public abstract List<RealVector> getClusterCenters();

    /**
     * @return weights of each cluster
     */
    public abstract double[] getClusterProportions();

    /**
     * @return covariances of mixture components
     */
    public abstract List<RealMatrix> getClusterCovariances();

    public abstract double[] getClusterProbabilities(Datum d);


    @Override
    public double getZScoreEquivalent(double zscore) {
        throw new NotImplementedException("Mixture Models don't have z-score equivalent");
    }

    public boolean checkTermination(double logLikelihood, double oldLogLikelihood, int iteration) {
        log.debug("log likelihood after iteration {} is {}", iteration, logLikelihood);

        if (iteration >= maxIterationsToConverge) {
            log.debug("Breaking because have already run {} iterations", iteration);
            return true;
        }

        double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
        if (improvement >= 0 && improvement < progressCutoff) {
            log.debug("Breaking because improvement was {} percent", improvement * 100);
            return true;
        } else {
            log.debug("improvement is : {}%", improvement * 100);
        }
        log.debug(".........................................");
        return false;
    }
}

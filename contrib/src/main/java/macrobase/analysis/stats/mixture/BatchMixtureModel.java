package macrobase.analysis.stats.mixture;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public abstract class BatchMixtureModel extends BatchTrainScore {
    private static final Logger log = LoggerFactory.getLogger(BatchMixtureModel.class);
    protected final double progressCutoff;
    protected final int maxIterationsToConverge;
    protected MacroBaseConf conf;
    protected double trainTestSplit;
    protected final String initialClusterCentersFile;

    public BatchMixtureModel(MacroBaseConf conf) {
        super(conf);
        this.conf = conf;
        progressCutoff = conf.getDouble(GMMConf.ITERATIVE_PROGRESS_CUTOFF_RATIO, GMMConf.ITERATIVE_PROGRESS_CUTOFF_RATIO_DEFAULT);
        maxIterationsToConverge = conf.getInt(GMMConf.MAX_ITERATIONS_TO_CONVERGE, GMMConf.MAX_ITERATIONS_TO_CONVERGE_DEFAULT);
        trainTestSplit = conf.getDouble(GMMConf.TRAIN_TEST_SPLIT, GMMConf.TRAIN_TEST_SPLIT_DEFAULT);
        log.debug("max iter = {}", maxIterationsToConverge);
        this.initialClusterCentersFile = conf.getString(GMMConf.MIXTURE_CENTERS_FILE, null);
    }

    public static List<RealVector> initializeClustersFromFile(String filename, int K) throws FileNotFoundException {
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(filename));
        RealVector[] centers = gson.fromJson(reader, ArrayRealVector[].class);
        List<RealVector> vectors = Arrays.asList(centers);
        if(vectors.size() > K) {
            return vectors.subList(0, K);
        } else {
            return vectors;
        }
    }

    protected static List<RealVector> gonzalezInitializeMixtureCenters(List<RealVector> pickedVectors, List<Datum> data, int K, Random rand) {
        List<RealVector> vectors = new ArrayList<>(pickedVectors);
        int index = rand.nextInt(data.size());
        for (int k = pickedVectors.size(); k < K; k++) {
            double maxDistance = 0;
            for (int n = 0; n < data.size(); n++) {
                double distance = 0;
                for (int j = 0; j < k; j++) {
                    distance += data.get(n).metrics().getDistance(vectors.get(j));
                }
                if (distance > maxDistance) {
                    maxDistance = distance;
                    index = n;
                }
            }
            vectors.add(data.get(index).metrics());
        }
        return vectors;
    }

    protected static List<RealVector> gonzalezInitializeMixtureCenters(List<Datum> data, int K, Random rand) {
        List<RealVector> vectors = new ArrayList<>(K);
        vectors.add(data.get(rand.nextInt(data.size())).metrics());
        return gonzalezInitializeMixtureCenters(vectors, data, K, rand);
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

    public boolean checkTermination(double logLikelihood, double oldLogLikelihood, int iteration) {
        log.debug("average point log likelihood after iteration {} is {}", iteration, logLikelihood);

        if (iteration >= maxIterationsToConverge) {
            log.debug("Breaking because have already run {} iterations", iteration);
            return true;
        }

        double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
        if (improvement >= 0 && improvement < progressCutoff) {
            log.debug("Breaking because improvement was {} percent", improvement * 100);
            return true;
        } else {
            log.debug("improvement is : {} percent", improvement * 100);
        }
        log.debug(".........................................");
        return false;
    }
}

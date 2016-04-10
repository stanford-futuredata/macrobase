package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

public abstract class BatchMixtureModel extends BatchTrainScore {
    public BatchMixtureModel(MacroBaseConf conf) {
        super(conf);
    }

    protected List<RealVector> gonzalezInitializeMixtureCenters(List<Datum> data, int K) {
        List<RealVector> vectors = new ArrayList<>(K);
        int N = data.size();
        Random rand = new Random();
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
}

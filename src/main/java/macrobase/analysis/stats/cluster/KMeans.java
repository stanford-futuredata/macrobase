package macrobase.analysis.stats.cluster;

import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class KMeans {
    private static final Logger log = LoggerFactory.getLogger(KMeans.class);

    private final int k;
    private final int maxIter = 10;

    public KMeans(int k) {
        this.k = k;
    }

    public List<RealVector> emIterate(List<Datum> data) {
        return emIterate(data, new Random());
    }

    public static List<Integer> getClusterAssignments(List<Datum> data, List<RealVector> centers) {
        return data.stream()
                .map((Datum d) -> centers.stream()
                        .map((RealVector v) -> v.getDistance(d.getMetrics()))
                        .collect(Collectors.toList()))
                .map(KMeans::argMin)
                .collect(Collectors.toList());
    }

    public List<RealVector> emIterate(List<Datum> data, Random rand) {
        List<RealVector> centers = randomK(data, k, rand);
        List<Integer> assigments = getClusterAssignments(data, centers);

        boolean differentAssignments = true;
        for (int iter = 1; differentAssignments && iter < maxIter; iter++) {
            log.debug("iter {}, centers: {}", iter, centers);
            List<List<RealVector>> clusterPoints = new ArrayList<>(k);
            for (int i = 0; i < k; i++) {
                clusterPoints.add(new ArrayList<>());
            }
            for (int i = 0; i < data.size(); i++) {
                clusterPoints.get(assigments.get(i)).add(data.get(i).getMetrics());
            }
            centers = clusterPoints.stream().map(AlgebraUtils::calcMean).collect(Collectors.toList());
            List<Integer> oldAssigments = assigments;
            assigments = getClusterAssignments(data, centers);

            differentAssignments = false;
            for (int i = 0; i < oldAssigments.size(); i++) {
                if (oldAssigments.get(i) != assigments.get(i)) {
                    differentAssignments = true;
                    break;
                }
            }
            if (!differentAssignments) {
                log.debug("assignments did not change");
            }
        }
        log.debug("returning: {}", centers);
        return centers;
    }

    public static int argMin(List<Double> doublesList) {
        double min = Double.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < doublesList.size(); i++) {
            if (doublesList.get(i) < min) {
                min = doublesList.get(i);
                minIndex = i;
            }
        }
        return minIndex;
    }

    public static List<RealVector> randomK(List<Datum> data, int k, Random rand) {
        List pickedVectors = new ArrayList<>(k);
        HashSet<Integer> pickedIndices = new HashSet<>();
        int index = rand.nextInt(data.size());
        for (int i = 0; i < k; i++) {
            while (pickedIndices.contains(index)) {
                index = rand.nextInt(data.size());
            }
            pickedVectors.add(data.get(index).getMetrics());
            pickedIndices.add(index);
        }
        return pickedVectors;
    }
}

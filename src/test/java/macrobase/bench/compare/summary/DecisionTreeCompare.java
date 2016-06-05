package macrobase.bench.compare.summary;

import com.google.common.collect.Sets;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DecisionTreeCompare extends SummaryCompare {
    private static final Logger log = LoggerFactory.getLogger(DecisionTreeCompare.class);

    private final int maxDepth;

    public DecisionTreeCompare(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    private class TreeNode {
        public int value;
        public double ratio;
        public TreeNode leftChild;
        public TreeNode rightChild;
    }

    public TreeNode root;

    public TreeNode getBestSplit(Set<Integer> ignoreValues, List<Datum> inliers, List<Datum> outliers) {
        // only works because values are hash coded to be integer
        HashMap<Integer, Integer> inlierCount = new HashMap<>();
        HashMap<Integer, Integer> outlierCount = new HashMap<>();

        for(Datum d : inliers) {
            for(Integer attr : d.getAttributes()) {
                inlierCount.compute(attr, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        for(Datum d : inliers) {
            for(Integer attr : d.getAttributes()) {
                outlierCount.compute(attr, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        int bestInfinityCount = -1;
        double bestRatio = Double.NEGATIVE_INFINITY;
        int bestValue = -0xDEADBEEF;
        for(Map.Entry<Integer, Integer> e : outlierCount.entrySet()) {
            if(ignoreValues.contains(e.getKey())) {
                continue;
            }

            Integer ic = inlierCount.get(e.getKey());
            if(ic == null) {
                bestRatio = Double.POSITIVE_INFINITY;

                if(e.getValue() > bestInfinityCount) {
                    bestValue = e.getKey();
                    bestInfinityCount = e.getValue();
                }
            } else {
                double ratio = ((double) e.getValue()) / ic;
                if (ratio > bestRatio) {
                    bestValue = e.getKey();
                    bestRatio = ratio;
                }
            }
        }

        if(bestValue == -0xDEADBEEF) {
            //log.debug("Best value was -1, {} {}", ignoreValues, outlierCount.keySet());
            return null;
        }

        TreeNode newNode = new TreeNode();
        newNode.ratio = bestRatio;
        newNode.value = bestValue;

        return newNode;
    }

    public TreeNode partition(int curDepth, Set<Integer> alreadyChosen, final int maxDepth, List<Datum> inliers, List<Datum> outliers) {
        if(curDepth == maxDepth) {
            return null;
        }

        TreeNode splitnode = getBestSplit(alreadyChosen, inliers, outliers);

        if(splitnode == null) {
            //  log.debug("ran out of outliers here, breaking {}", curDepth);
            return splitnode;
        }

        //log.debug("best split was {} count was {}", splitnode.value, splitnode.ratio);

        List<Datum> leftInliers = new ArrayList<>();
        List<Datum> leftOutliers = new ArrayList<>();

        List<Datum> rightInliers = new ArrayList<>();
        List<Datum> rightOutliers = new ArrayList<>();

        for(Datum d : inliers) {
            if(d.getAttributes().contains(splitnode.value)) {
                leftInliers.add(d);
            } else {
                rightInliers.add(d);
            }
        }

        for(Datum d : outliers) {
            if(d.getAttributes().contains(splitnode.value)) {
                leftOutliers.add(d);
            } else {
                rightOutliers.add(d);
            }
        }

        alreadyChosen = Sets.newHashSet(alreadyChosen);
        alreadyChosen.add(splitnode.value);

        if(leftInliers.size() > 0 && leftOutliers.size() > 0) {
            splitnode.leftChild = partition(curDepth + 1, alreadyChosen, maxDepth, leftInliers, leftOutliers);
        } else {
            //log.debug("Ended left split at {}", curDepth);
        }

        if(rightInliers.size() > 0 && rightOutliers.size() > 0) {
            splitnode.rightChild = partition(curDepth + 1, alreadyChosen, maxDepth, rightInliers, rightOutliers);
        } else {
            //log.debug("Ended right split at {}", curDepth);
        }

        return splitnode;
    }

    public Map<Set<Integer>, Integer> compare(List<Datum> inliers,
                                              List<Datum> outliers) {

        partition(0, new HashSet<>(), maxDepth, inliers, outliers);

        return new HashMap<>();
    }
}
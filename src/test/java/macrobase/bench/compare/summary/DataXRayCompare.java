package macrobase.bench.compare.summary;

import com.google.common.collect.Sets;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataXRayCompare extends SummaryCompare {
    private static final Logger log = LoggerFactory.getLogger(DataXRayCompare.class);


    private final double alpha = 0.5;
    private Map<Set<Integer>, Double> computedScores = new HashMap<>();
    private Set<Set<Integer>> coveredSets = new HashSet<>();

    private double cost(Set<Integer> attrs, List<Datum> inliers, List<Datum> outliers) {
        int matchingInliers = countMatches(attrs, inliers);
        int matchingOutliers = countMatches(attrs, outliers);

        double errorRate = matchingOutliers != 0 ? ((double)matchingInliers)/(matchingInliers+matchingOutliers) : 0;
        if(errorRate > 0 && errorRate < 1)
            return Math.log(1/alpha) + matchingOutliers*Math.log(1/errorRate) + matchingInliers*Math.log(1/(1-errorRate));
            // matches Xray code
        else
            return Math.log(1/alpha);
    }

    private int countMatches(Set<Integer> attrs, List<Datum> data) {
        int cnt = 0;
        for(Datum i : data) {
            boolean matched = true;
            for(Integer attr : attrs) {
                if(!i.getAttributes().contains(attr)) {
                    matched = false;
                    break;
                }
            }

            if(matched) {
                cnt += 1;
            }
        }

        return cnt;
    }

    Map<Integer, Set<Integer>> allAttributes = new HashMap<>();

    private class Candidate {
        Set<Integer> attributeSet;
        Set<Integer> dimsUsed;
    }

    private Set<Candidate> getChildren(Set<Integer> boundDimensions, Set<Integer> baseSet) {
        Set<Candidate> ret = Sets.newHashSet();
        for(Integer dim : allAttributes.keySet()) {
            if(boundDimensions.contains(dim)) {
                continue;
            }

            for(Integer attr : allAttributes.get(dim)) {
                Set<Integer> newSet = Sets.newHashSet(baseSet);
                newSet.add(attr);
                if(!coveredSets.contains(newSet)) {
                    Candidate c = new Candidate();
                    c.attributeSet = newSet;
                    Set<Integer> dimSet = Sets.newHashSet(boundDimensions);
                    dimSet.add(dim);
                    c.dimsUsed = dimSet;
                    ret.add(c);
                }
            }
        }

        return ret;
    }

    public Map<Set<Integer>, Integer> compare(List<Datum> inliers,
                                              List<Datum> outliers) {

        final int attrSize = inliers.get(0).getAttributes().size();
        for (int i = 0; i < attrSize; ++i) {
            allAttributes.put(i, new HashSet<>());
        }

        for (Datum d : inliers) {
            List<Integer> attrs = d.getAttributes();
            for (int i = 0; i < attrs.size(); ++i) {
                allAttributes.get(i).add(attrs.get(i));
            }
        }

        for (Datum d : outliers) {
            List<Integer> attrs = d.getAttributes();
            for (int i = 0; i < attrs.size(); ++i) {
                allAttributes.get(i).add(attrs.get(i));
            }
        }

        Set<Integer> rootSet = new HashSet<>();
        double rootScore = cost(rootSet, inliers, outliers);
        computedScores.put(rootSet, rootScore);

        recurse(rootScore, new HashSet<>(), new HashSet<>(), inliers, outliers, true);

        log.debug("Processed {} candidates", computedScores.size());

        return new HashMap<>();
    }


    private void recurse(double parentScore,
                         Set<Integer> boundDimensions,
                         Set<Integer> currentAttributes,
                         List<Datum> inliers,
                         List<Datum> outliers,
                         boolean first) {


        Set<Candidate> children = getChildren(boundDimensions, currentAttributes);

        if(children.size() == 0) {
            return;
        }

        double childSum = 0;
        for(Candidate child : children) {
            Double childScore = computedScores.get(child.attributeSet);
            if(childScore == null) {
                childScore = cost(child.attributeSet, inliers, outliers);
                computedScores.put(child.attributeSet, childScore);
            }

            childSum += childScore;
        }

        if(! first && childSum > parentScore) {
            for(Candidate child : children) {
                coveredSets.add(child.attributeSet);
            }
        } else {
            for(Candidate child : children) {
                if(!coveredSets.contains(child.attributeSet)) {
                    recurse(computedScores.get(child.attributeSet),
                            child.dimsUsed,
                            child.attributeSet,
                            inliers,
                            outliers,
                            false);
                }
            }
        }
    }
}
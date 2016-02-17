package macrobase.analysis.summary.itemset;

import com.google.common.collect.Lists;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;

import java.util.*;

public class Apriori {
    private Set<Set<Integer>> genCandidates(List<ItemsetWithCount> prevRound,
                                            int desiredSize) {
        Set<Set<Integer>> ret = new HashSet<>();
        for (int i = 0; i < prevRound.size(); ++i) {
            for (int j = i + 1; j < prevRound.size(); ++j) {
                Set<Integer> combined = new HashSet<>();
                combined.addAll(prevRound.get(i).getItems());
                combined.addAll(prevRound.get(j).getItems());
                if (combined.size() == desiredSize) {
                    ret.add(combined);
                }
            }
        }

        return ret;
    }

    private List<ItemsetWithCount> filterItems(List<Set<Integer>> transactions,
                                               Set<Set<Integer>> candidates,
                                               Set<Integer> infrequentIndex,
                                               int minSupportCount) {
        List<ItemsetWithCount> ret = new ArrayList<>();

        HashMap<Set<Integer>, Integer> candidateCounts = new HashMap<>();

        for (int i = 0; i < transactions.size(); ++i) {
            if (infrequentIndex.contains(i)) {
                continue;
            }

            Set<Integer> txn = transactions.get(i);
            boolean foundSupportInTxn = false;
            for (Set<Integer> candidate : candidates) {
                boolean allFound = true;
                for (Integer candidateItem : candidate) {
                    if (!txn.contains(candidateItem)) {
                        allFound = false;
                        break;
                    }
                }

                if (allFound) {
                    candidateCounts.compute(candidate, (k, v) -> v == null ? 1 : v + 1);
                    foundSupportInTxn = true;
                }
            }

            if (!foundSupportInTxn) {
                infrequentIndex.add(i);
            }
        }

        for (Map.Entry<Set<Integer>, Integer> e : candidateCounts.entrySet()) {
            if (e.getValue() >= minSupportCount) {
                ret.add(new ItemsetWithCount(e.getKey(), e.getValue()));
            }
        }
        return ret;
    }

    public Set<ItemsetWithCount> getItemsets(List<Set<Integer>> transactions,
                                             Double support) {
        Set<ItemsetWithCount> ret = new HashSet<>();

        int minSupportCount = (int) (support * transactions.size());

        // first round candidates are all items; just count them
        HashMap<Integer, Integer> itemCounts = new HashMap<>();
        for (Set<Integer> t : transactions) {
            for (int i : t) {
                itemCounts.compute(i, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        for (Map.Entry<Integer, Integer> e : itemCounts.entrySet()) {
            if (e.getValue() >= minSupportCount) {
                HashSet<Integer> singletonSet = new HashSet<>();
                singletonSet.add(e.getKey());
                ret.add(new ItemsetWithCount(singletonSet, e.getValue()));
            }
        }

        if (ret.size() == 0) {
            return ret;
        }

        // second round, don't explicitly construct pairs
        HashMap<Set<Integer>, Integer> pairCandidateCounts = new HashMap<>();

        for (Set<Integer> t : transactions) {
            List<Integer> txList = Lists.newArrayList(t);
            for (int i = 0; i < t.size(); ++i) {
                for (int j = i + 1; j < t.size(); ++j) {
                    HashSet<Integer> pairSet = new HashSet<>();
                    pairSet.add(txList.get(i));
                    pairSet.add(txList.get(j));
                    pairCandidateCounts.compute(pairSet, (k, v) -> v == null ? 1 : v + 1);
                }
            }
        }

        List<ItemsetWithCount> pairItemsets = new ArrayList<>();

        for (Map.Entry<Set<Integer>, Integer> e : pairCandidateCounts.entrySet()) {
            if (e.getValue() >= minSupportCount) {
                ItemsetWithCount ic = new ItemsetWithCount(e.getKey(), e.getValue());
                ret.add(ic);
                pairItemsets.add(ic);
            }
        }

        if (pairItemsets.isEmpty()) {
            return ret;
        }

        List<ItemsetWithCount> prevRoundItemsets = pairItemsets;
        Set<Integer> infrequentIndex = new HashSet<>();

        int newSize = 3;
        while (true) {
            Set<Set<Integer>> candidates = genCandidates(prevRoundItemsets, newSize);
            prevRoundItemsets = filterItems(transactions,
                                            candidates,
                                            infrequentIndex,
                                            minSupportCount);
            if (prevRoundItemsets.isEmpty()) {
                return ret;
            } else {
                ret.addAll(prevRoundItemsets);
                newSize += 1;
            }
        }
    }
}

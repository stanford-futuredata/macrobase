package macrobase.analysis.summary.itemset;

import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class FPGrowthTest {
    private Set<Integer> intIfy(String txnStr) {
        return Arrays.stream(txnStr.split(", ")).map(s -> (int) s.charAt(0)).collect(Collectors.toSet());
    }

    @SuppressWarnings("unused")
    private void printItemsets(List<ItemsetWithCount> itemsets) {
        itemsets.sort((a, b) -> b.getItems().size() - a.getItems().size());
        for (ItemsetWithCount i : itemsets) {
            System.out.format("\ncount %f, size %d\n", i.getCount(), i.getItems().size());
            for (int item : i.getItems()) {
                System.out.println((char) item);
            }
        }
    }

    private boolean compareResults(Set<ItemsetWithCount> ap_itemsets, List<ItemsetWithCount> itemsets) {
        for (int i = 0; i < itemsets.size(); i ++) {
            boolean foundEquals = false;
            for (ItemsetWithCount iwc : ap_itemsets) {
                foundEquals |= iwc.equals(itemsets.get(i));
            }
            if (!foundEquals) { return false; };
        }
        return true;
    }

    @Test
    public void testFPFromPaper() {
        List<Set<Integer>> txns = new ArrayList<>();
        txns.add(intIfy("f, a, c, d, g, i, m, p"));
        txns.add(intIfy("a, b, c, f, l, m, o"));
        txns.add(intIfy("b, f, h, j, o"));
        txns.add(intIfy("b, c, k, s, p"));
        txns.add(intIfy("a, f, c, e, l, p, m, n"));

        FPGrowth fp = new FPGrowth();
        Apriori ap = new Apriori();

        Set<ItemsetWithCount> ap_itemsets = ap.getItemsets(txns, .6);
        List<ItemsetWithCount> itemsets = fp.getItemsetsWithSupportRatio(txns, .6);

        assertEquals(18, itemsets.size());
        assert(compareResults(ap_itemsets, itemsets));
    }

    @Test
    public void testFPLonger() {

        List<Set<Integer>> txns = new ArrayList<>();
        txns.add(intIfy("f, a, c, d, g, i, m, p"));
        txns.add(intIfy("a, b, c, f, l, m, o"));

        FPGrowth fp = new FPGrowth();

        List<ItemsetWithCount> itemsets = fp.getItemsetsWithSupportRatio(txns, .2);

        Apriori ap = new Apriori();
        Set<ItemsetWithCount> api = ap.getItemsets(txns, .2);

        //printItemsets(itemsets);

        List<Set<Integer>> apil = api.stream().map(i -> i.getItems()).collect(Collectors.toList());
        Set<Set<Integer>> dupdetector = new HashSet<>();

        for (Set<Integer> s : apil) {
            if (!dupdetector.add(s)) {
//                log.warn("DUPLICATE APRIORI SET {}", s);
            }
        }

        Set<Set<Integer>> iss = itemsets.stream().map(i -> i.getItems()).collect(Collectors.toSet());

//        log.debug("DIFF: {}", Sets.difference(dupdetector, iss));

        assertEquals(api.size(), itemsets.size());
    }

    @Test
    public void simpleTest() {
        List<Set<Integer>> txns = new ArrayList<>();
        txns.add(intIfy("a, b, c"));
        txns.add(intIfy("a, b"));
        txns.add(intIfy("a"));

        FPGrowth fp = new FPGrowth();
        Apriori ap = new Apriori();

        Set<ItemsetWithCount> ap_itemsets = ap.getItemsets(txns, .7);
        List<ItemsetWithCount> itemsets = fp.getItemsetsWithSupportRatio(txns, .7);

        //printItemsets(itemsets);
        assertEquals(3, itemsets.size());
        assert(compareResults(ap_itemsets, itemsets));
    }

    @Test
    public void dupTest() {
        List<Set<Integer>> txns = new ArrayList<>();
        txns.add(intIfy("a, c, d"));
        txns.add(intIfy("a, c, d, e"));
        txns.add(intIfy("c"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("b"));
        txns.add(intIfy("b"));
        txns.add(intIfy("b"));
        txns.add(intIfy("a, b, d"));
        txns.add(intIfy("a, b, e, c"));

        FPGrowth fp = new FPGrowth();

        List<ItemsetWithCount> itemsets = fp.getItemsetsWithSupportCount(txns, null, .01 * txns.size(), true);

        List<Set<Integer>> apil = itemsets.stream().map(i -> i.getItems()).collect(Collectors.toList());
        Set<Set<Integer>> dupdetector = new HashSet<>();

        for (Set<Integer> s : apil) {
            if (!dupdetector.add(s)) {
//                log.warn("DUPLICATE FPTREE SET {}", s);
            }
        }

        //printItemsets(itemsets);

        assertEquals(dupdetector.size(), itemsets.size());
    }

    @Test
    public void testGetSupport() {
        List<Set<Integer>> txns = new ArrayList<>();
        txns.add(intIfy("a, c, d"));
        txns.add(intIfy("a, c, d, e"));
        txns.add(intIfy("c"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("a"));
        txns.add(intIfy("b"));
        txns.add(intIfy("b"));
        txns.add(intIfy("b"));
        txns.add(intIfy("a, b, d"));
        txns.add(intIfy("a, b, e, c"));

        FPGrowth.FPTree fpt = new FPGrowth().constructTree(txns, 0);
//        fpt.printTreeDebug();

        assertEquals(2, fpt.getSupport(intIfy("a, b")));
        assertEquals(0, fpt.getSupport(intIfy("a, b, c, d")));

    }
}

class Apriori {
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
            List<Integer> txList = new ArrayList<>(t);
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

package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.ItemsetWithCount;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class FPGrowthGroupedTest {
    private ItemsetWithCount intIfy(String txnStr) {
        return intIfy(txnStr, 1);
    }
    private ItemsetWithCount intIfy(String txnStr, int count) {
        Set<Integer> set = Arrays.stream(txnStr.split(", ")).map(s -> (int) s.charAt(0)).collect(Collectors.toSet());
        return new ItemsetWithCount(set, count);
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
        List<ItemsetWithCount> txns = new ArrayList<>();
        txns.add(intIfy("f, a, c, d, g, i, m, p"));
        txns.add(intIfy("a, b, c, f, l, m, o"));
        txns.add(intIfy("b, f, h, j, o"));
        txns.add(intIfy("b, c, k, s, p"));
        txns.add(intIfy("a, f, c, e, l, p, m, n"));

        FPGrowthGrouped fp = new FPGrowthGrouped();
        Apriori ap = new Apriori();

        List<ItemsetWithCount> itemsets = fp.getItemsetsWithSupportRatio(txns, .6);

        assertEquals(18, itemsets.size());
    }

    @Test
    public void simpleTest() {
        List<ItemsetWithCount> txns = new ArrayList<>();
        txns.add(intIfy("a, b, c"));
        txns.add(intIfy("a, b"));
        txns.add(intIfy("a", 2));

        List<Set<Integer>> rawTxns = new ArrayList<>();
        for (ItemsetWithCount t : txns) {
            for (int i = 0; i < t.getCount(); i++) {
                rawTxns.add(t.getItems());
            }
        }

        FPGrowthGrouped fp = new FPGrowthGrouped();
        Apriori ap = new Apriori();

        Set<ItemsetWithCount> ap_itemsets = ap.getItemsets(rawTxns, .7);
        List<ItemsetWithCount> itemsets = fp.getItemsetsWithSupportRatio(txns, .7);

        //printItemsets(itemsets);
        assertEquals(3, itemsets.size());
        assert(compareResults(ap_itemsets, itemsets));
    }

    @Test
    public void dupTest() {
        List<ItemsetWithCount> txns = new ArrayList<>();
        txns.add(intIfy("a, c, d"));
        txns.add(intIfy("a, c, d, e"));
        txns.add(intIfy("c"));
        txns.add(intIfy("a",2));
        txns.add(intIfy("a"));
        txns.add(intIfy("b", 3));
        txns.add(intIfy("a, b, d"));
        txns.add(intIfy("a, b, e, c"));

        FPGrowthGrouped fp = new FPGrowthGrouped();

        List<ItemsetWithCount> itemsets = fp.getItemsetsWithSupportCount(txns, null, .01 * txns.size(), true);

        List<Set<Integer>> apil = itemsets.stream().map(i -> i.getItems()).collect(Collectors.toList());
        Set<Set<Integer>> dupdetector = new HashSet<>();

        int numDup = 0;
        for (Set<Integer> s : apil) {
            if (!dupdetector.add(s)) {
                numDup++;
            }
        }
        assertEquals(0, numDup);

//        printItemsets(itemsets);

        assertEquals(dupdetector.size(), itemsets.size());
    }

    @Test
    public void testGetSupport() {
        List<ItemsetWithCount> txns = new ArrayList<>();
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

        FPGrowthGrouped.FPTree fpt = new FPGrowthGrouped().constructTree(txns, 0);
//        fpt.printTreeDebug();

        assertEquals(2, fpt.getSupport(intIfy("a, b").getItems()));
        assertEquals(0, fpt.getSupport(intIfy("a, b, c, d").getItems()));

    }
}


package macrobase.analysis.summary.itemset;

import com.google.common.collect.Lists;

import macrobase.analysis.summary.itemset.Apriori;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AprioriTest {
    private Set<Integer> intIfy(String txnStr) {
        return Arrays.stream(txnStr.split(", ")).map(s -> (int) s.charAt(0)).collect(Collectors.toSet());
    }

    @SuppressWarnings("unused")
    private void printItemsets(Set<ItemsetWithCount> itemsetsSet) {
        List<ItemsetWithCount> itemsets = Lists.newArrayList(itemsetsSet);

        itemsets.sort((a, b) -> b.getItems().size() - a.getItems().size());
        for (ItemsetWithCount i : itemsets) {
            System.out.format("\ncount %d, size %d\n", i.getCount(), i.getItems().size());
            for (int item : i.getItems()) {
                System.out.println((char) item);
            }
        }
    }

    @Test
    public void testFPFromPaper() {
        List<Set<Integer>> txns = new ArrayList<>();
        txns.add(intIfy("f, a, c, d, g, i, m, p"));
        txns.add(intIfy("a, b, c, f, l, m, o"));
        txns.add(intIfy("b, f, h, j, o"));
        txns.add(intIfy("b, c, k, s, p"));
        txns.add(intIfy("a, f, c, e, l, p, m, n"));

        Apriori fp = new Apriori();

        Set<ItemsetWithCount> itemsets = fp.getItemsets(txns, .6);

        //printItemsets(itemsets);

        assertEquals(18, itemsets.size());
    }

    @Test
    public void simpleTest() {
        List<Set<Integer>> txns = new ArrayList<>();
        txns.add(intIfy("a, b, c"));
        txns.add(intIfy("a, b"));
        txns.add(intIfy("a"));

        Apriori fp = new Apriori();

        Set<ItemsetWithCount> itemsets = fp.getItemsets(txns, .7);

        //printItemsets(itemsets);

        assertEquals(3, itemsets.size());
    }
}

package macrobase.analysis.summary.itemset;

import com.google.common.collect.Lists;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Created by pbailis on 12/15/15.
 */
public class StreamingFPGrowthTest {

    private static final Logger log = LoggerFactory.getLogger(StreamingFPGrowthTest.class);

    private Set<Integer> intIfy(String txnStr) {
        return Arrays.stream(txnStr.split(", ")).map(s -> (int) s.charAt(0)).collect(Collectors.toSet());
    }

    private void printItemsets(List<ItemsetWithCount> itemsets) {
        itemsets.sort((a, b) -> b.getItems().size() - a.getItems().size());
        for (ItemsetWithCount i : itemsets) {
            System.out.format("\ncount %f, size %d\n", i.getCount(), i.getItems().size());
            for (int item : i.getItems()) {
                System.out.println((char) item);
            }
        }
    }

    @Test
    public void testFPFromPaper() {
        List<Set<Integer>> allTxns = new ArrayList<>();
        allTxns.add(intIfy("f, a, c, d, g, i, m, p"));
        allTxns.add(intIfy("a, b, c, f, l, m, o"));
        allTxns.add(intIfy("b, f, h, j, o"));
        allTxns.add(intIfy("b, c, k, s, p"));
        allTxns.add(intIfy("a, f, c, e, l, p, m, n"));

        StreamingFPGrowth fp = new StreamingFPGrowth(.2);

        fp.buildTree(allTxns);
        List<ItemsetWithCount> itemsets;
        //itemsets = fp.getItemsetsWithSupportRatio();

        //printItemsets(itemsets);

        List<Set<Integer>> newBatch = new ArrayList<>();
        newBatch.add(intIfy("a, b, c, d, e"));
        newBatch.add(intIfy("b, a, d, a, s, s,"));
        newBatch.add(intIfy("d, a, t, t, h, i, n, g"));
        newBatch.add(intIfy("f, a, k, s, p, e"));

        allTxns.addAll(newBatch);

        fp.insertTransactionsStreamingExact(newBatch);

        itemsets = fp.getItemsets();

        FPGrowth apriori = new FPGrowth();

        List<ItemsetWithCount> apItemsets = apriori.getItemsetsWithSupportRatio(allTxns, .2);

        Set<Set<Integer>> apis = apItemsets.stream().map(i -> i.getItems()).collect(Collectors.toSet());

        List<Set<Integer>> apil = apItemsets.stream().map(i -> i.getItems()).collect(Collectors.toList());
        Set<Set<Integer>> dupdetector = new HashSet<>();

        for (Set<Integer> s : apil) {
            if (!dupdetector.add(s)) {
                log.warn("DUPLICATE FPTREE SET {}", s);
            }
        }


        /*
        Set<Set<Integer>> iss = itemsets.stream().map(i -> i.getItems()).collect(Collectors.toSet());

        List<Set<Integer>> issl = itemsets.stream().map(i -> i.getItems()).collect(Collectors.toList());
        Set<Set<Integer>> dupdetector2 = new HashSet<>();

        for(Set<Integer> s : issl) {
            if(!dupdetector2.add(s)) {
                log.warn("DUPLICATE STREAM SET {}", s);
            }
        }

        Set<Set<Integer>> diff = Sets.difference(apis, iss);
        log.debug("DIFF: {}\ndiff size: {}, fptree sets size: {}, fptree list size: {}", diff, diff.size(), apis.size(),
                  apItemsets.size());
        */

        assertEquals(apItemsets.size(), itemsets.size());
    }

    @Test
    public void simpletest() {
        List<Set<Integer>> allTxns = new ArrayList<>();
        allTxns.add(intIfy("a, b, c"));
        allTxns.add(intIfy("a, b"));

        StreamingFPGrowth fp = new StreamingFPGrowth(.5);

        fp.buildTree(allTxns);
        List<ItemsetWithCount> itemsets;
        //itemsets = fp.getItemsetsWithSupportRatio();

        //printItemsets(itemsets);

        List<Set<Integer>> newBatch = new ArrayList<>();
        newBatch.add(intIfy("c, d"));
        newBatch.add(intIfy("a, d"));
        newBatch.add(intIfy("a, d, e"));

        allTxns.addAll(newBatch);

        fp.insertTransactionsStreamingExact(newBatch);

        itemsets = fp.getItemsets();

        Apriori apriori = new Apriori();

        Set<ItemsetWithCount> apItemsets = apriori.getItemsets(allTxns, .5);

        /*
        System.out.printf("%d %d\n", itemsets.size(), apItemsets.size());

        System.out.println("FPTREE");
        printItemsets(itemsets);

        System.out.println("APRIORI");
        printItemsets(Lists.newArrayList(apItemsets));
        */

        assertEquals(apItemsets.size(), itemsets.size());

        fp.printTreeDebug();

        assertEquals(2,
                     fp.getCounts(
                             Lists.newArrayList(new ItemsetWithCount(intIfy("a, b"), 1000)))
                             .get(0).getCount(), 0);
    }

    @Test
    public void stress() {
        StreamingFPGrowth fp = new StreamingFPGrowth(.001);
        Random random = new Random(0);
        int cnt = 0;

        Map<Integer, Double> frequentItems = new HashMap<>();
        for (cnt = 0; cnt <= 1000; ++cnt) {
            int itemSetSize = random.nextInt(100);
            Set<Integer> itemSet = new HashSet<>(itemSetSize);
            for (int i = 0; i < itemSetSize; ++i) {
                itemSet.add(random.nextInt(100));
                frequentItems.compute(i, (k, v) -> v == null ? 1 : v + 1);
            }

            fp.insertTransactionStreamingFalseNegative(itemSet);

            if (cnt % 20 == 0) {
                int toDecay = random.nextInt(frequentItems.size());
                for (int i = 0; i < toDecay; ++i) {
                    frequentItems.remove(frequentItems.keySet().toArray()[random.nextInt(frequentItems.size())]);
                }
                fp.decayAndResetFrequentItems(frequentItems, .95);
            }
        }
    }
}

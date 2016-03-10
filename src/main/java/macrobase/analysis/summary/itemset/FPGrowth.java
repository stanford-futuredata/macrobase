package macrobase.analysis.summary.itemset;

import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.MacroBase;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class FPGrowth {
    private static final Logger log = LoggerFactory.getLogger(FPGrowth.class);
    private final Timer singleItemCounts = MacroBase.metrics.timer(name(FPGrowth.class, "itemCounts"));
    private final Timer insertTransactions = MacroBase.metrics.timer(name(FPGrowth.class, "insertTransactions"));
    private final Timer fpMine = MacroBase.metrics.timer(name(FPGrowth.class, "fpMine"));


    class FPTree {
        private FPTreeNode root = new FPTreeNode(-1, null, 0);
        // used to calculate the order
        private Map<Integer, Double> frequentItemCounts = new HashMap<>();

        // item order -- need canonical to break ties; 0 is smallest, N is largest
        private Map<Integer, Integer> frequentItemOrder = new HashMap<>();

        protected Map<Integer, FPTreeNode> nodeHeaders = new HashMap<>();

        protected void printTreeDebug() {
            log.debug("Frequent Item Counts:");
            frequentItemCounts.entrySet().forEach(e -> log.debug(String.format("%d: %f", e.getKey(), e.getValue())));

            walkTree(root, 1);
        }

        private void walkTree(FPTreeNode start, int treeDepth) {
            log.debug(String.format("%s node: %d, count: %f",
                                    new String(new char[treeDepth]).replaceAll("\0", "\t"),
                                    start.getItem(), start.getCount()));
            if (start.getChildren() != null) {
                for (FPTreeNode child : start.getChildren()) {
                    walkTree(child, treeDepth + 1);
                }
            }
        }

        private class FPTreeNode {
            private int item;
            private double count;
            private FPTreeNode nextLink;
            private FPTreeNode parent;
            private List<FPTreeNode> children;

            public FPTreeNode(int item, FPTreeNode parent, int initialCount) {
                this.item = item;
                this.parent = parent;
                this.count = initialCount;
            }

            public int getItem() {
                return item;
            }

            public double getCount() {
                return count;
            }

            public void incrementCount(double by) {
                count += by;
            }

            public void setNextLink(FPTreeNode nextLink) {
                this.nextLink = nextLink;
            }

            public FPTreeNode getNextLink() {
                return nextLink;
            }

            public FPTreeNode getParent() {
                return parent;
            }


            public List<FPTreeNode> getChildren() {
                return children;
            }

            // insert the transaction at this node starting with transaction[currentIndex]
            // then find the child that matches
            public void insertTransaction(List<Integer> fullTransaction,
                                          int currentIndex,
                                          final double transactionCount) {
                incrementCount(transactionCount);

                if (currentIndex == fullTransaction.size()) {
                    return;
                }

                int currentItem = fullTransaction.get(currentIndex);

                FPTreeNode matchingChild = null;

                if (children != null) {
                    for (FPTreeNode child : children) {
                        if (child.getItem() == currentItem) {
                            matchingChild = child;
                            break;
                        }
                    }
                }

                if (matchingChild == null) {
                    matchingChild = new FPTreeNode(currentItem, this, 0);

                    FPTreeNode prevHeader = nodeHeaders.get(currentItem);
                    nodeHeaders.put(currentItem, matchingChild);

                    if (prevHeader != null) {
                        matchingChild.setNextLink(prevHeader);
                    }

                    if (children == null) {
                        children = new ArrayList<>();
                    }

                    children.add(matchingChild);
                }

                matchingChild.insertTransaction(fullTransaction, currentIndex + 1, transactionCount);
            }
        }

        public void setFrequentCounts(Map<Integer, Double> counts) {
            frequentItemCounts = counts;
            sortFrequentItems();
        }

        public void insertFrequentItems(List<Set<Integer>> transactions,
                                        int countRequiredForSupport) {

            Map<Integer, Double> itemCounts = new HashMap<>();
            for (Set<Integer> t : transactions) {
                for (Integer item : t) {
                    itemCounts.compute(item, (k, v) -> v == null ? 1 : v + 1);
                }
            }

            for (Map.Entry<Integer, Double> e : itemCounts.entrySet()) {
                if (e.getValue() >= countRequiredForSupport) {
                    frequentItemCounts.put(e.getKey(), e.getValue());
                }
            }

            sortFrequentItems();
        }

        private void sortFrequentItems() {
            // we have to materialize a canonical order so that items with equal counts
            // are consistently ordered when they are sorted during transaction insertion
            List<Map.Entry<Integer, Double>> sortedItemCounts = Lists.newArrayList(frequentItemCounts.entrySet());
            sortedItemCounts.sort((i1, i2) -> frequentItemCounts.get(i1.getKey())
                    .compareTo(frequentItemCounts.get(i2.getKey())));
            for (int i = 0; i < sortedItemCounts.size(); ++i) {
                frequentItemOrder.put(sortedItemCounts.get(i).getKey(), i);
            }
        }

        public void insertConditionalFrequentItems(List<ItemsetWithCount> patterns,
                                                   int countRequiredForSupport) {
            Map<Integer, Double> itemCounts = new HashMap<>();

            for (ItemsetWithCount i : patterns) {
                for (Integer item : i.getItems()) {
                    itemCounts.compute(item, (k, v) -> v == null ? i.getCount() : v + i.getCount());
                }
            }

            for (Map.Entry<Integer, Double> e : itemCounts.entrySet()) {
                if (e.getValue() >= countRequiredForSupport) {
                    frequentItemCounts.put(e.getKey(), e.getValue());
                }
            }

            // we have to materialize a canonical order so that items with equal counts
            // are consistently ordered when they are sorted during transaction insertion
            List<Map.Entry<Integer, Double>> sortedItemCounts = Lists.newArrayList(frequentItemCounts.entrySet());
            sortedItemCounts.sort((i1, i2) -> frequentItemCounts.get(i1.getKey())
                    .compareTo(frequentItemCounts.get(i2.getKey())));
            for (int i = 0; i < sortedItemCounts.size(); ++i) {
                frequentItemOrder.put(sortedItemCounts.get(i).getKey(), i);
            }
        }

        public void insertDatum(List<Datum> datums) {
            for (Datum d : datums) {
                List<Integer> filtered = d.getAttributes().stream().filter(
                        i -> frequentItemCounts.containsKey(i)).collect(Collectors.toList());

                if (!filtered.isEmpty()) {
                    filtered.sort((i1, i2) -> frequentItemOrder.get(i2).compareTo(frequentItemOrder.get(i1)));
                    root.insertTransaction(filtered, 0, 1);
                }
            }
        }


        public void insertConditionalFrequentPatterns(List<ItemsetWithCount> patterns) {
            for (ItemsetWithCount is : patterns) {
                List<Integer> filtered = is.getItems().stream().filter(i -> frequentItemCounts.containsKey(i)).collect(
                        Collectors.toList());
                filtered.sort((i1, i2) -> frequentItemOrder.get(i2).compareTo(frequentItemOrder.get(i1)));
                root.insertTransaction(filtered, 0, is.getCount());
            }
        }

        public void insertTransactions(List<Set<Integer>> transactions) {
            for (Set<Integer> t : transactions) {
                List<Integer> filtered = t.stream().filter(i -> frequentItemCounts.containsKey(i)).collect(
                        Collectors.toList());

                if (!filtered.isEmpty()) {
                    filtered.sort((i1, i2) -> frequentItemOrder.get(i2).compareTo(frequentItemOrder.get(i1)));
                    root.insertTransaction(filtered, 0, 1);
                }
            }
        }

        public int getSupport(Set<Integer> pattern) {
            for (Integer i : pattern) {
                if (!frequentItemCounts.containsKey(i)) {
                    return 0;
                }
            }

            List<Integer> plist = Lists.newArrayList(pattern);
            // traverse bottom to top
            plist.sort((i1, i2) -> frequentItemOrder.get(i1).compareTo(frequentItemOrder.get(i2)));

            int count = 0;
            FPTreeNode pathHead = nodeHeaders.get(plist.get(0));
            while (pathHead != null) {
                FPTreeNode curNode = pathHead;
                int itemsToFind = plist.size();

                while (curNode != null) {
                    if (pattern.contains(curNode.getItem())) {
                        itemsToFind -= 1;
                    }

                    if (itemsToFind == 0) {
                        count += pathHead.count;
                        break;
                    }

                    curNode = curNode.getParent();
                }
                pathHead = pathHead.getNextLink();
            }

            return count;
        }


        List<ItemsetWithCount> mineItemsets(Integer supportCountRequired) {
            List<ItemsetWithCount> singlePathItemsets = new ArrayList<>();
            List<ItemsetWithCount> branchingItemsets = new ArrayList<>();

            // mine single-path itemsets first
            FPTreeNode curNode = root;
            FPTreeNode nodeOfBranching = null;
            Set<FPTreeNode> singlePathNodes = new HashSet<>();
            while (true) {
                if (curNode.children != null && curNode.children.size() > 1) {
                    nodeOfBranching = curNode;
                    break;
                }

                if (curNode != root) {
                    singlePathNodes.add(curNode);
                }

                if (curNode.children == null || curNode.children.size() == 0) {
                    break;
                } else {
                    curNode = curNode.children.get(0);
                }
            }

            for (Set<FPTreeNode> subset : Sets.powerSet(singlePathNodes)) {
                if (subset.isEmpty()) {
                    continue;
                }

                double minSupportInSubset = -1;
                Set<Integer> items = new HashSet<>();
                for (FPTreeNode n : subset) {
                    items.add(n.getItem());

                    if (minSupportInSubset == -1 || n.getCount() < minSupportInSubset) {
                        minSupportInSubset = n.getCount();
                    }
                }

                assert (minSupportInSubset >= supportCountRequired);
                singlePathItemsets.add(new ItemsetWithCount(items, minSupportInSubset));
            }

            // the entire tree was a single path...
            if (nodeOfBranching == null) {
                return singlePathItemsets;
            }

            // all of the items in the single path will have been mined now
            // due to the descending frequency count of the FPTree structure, so
            // we remove them from consideration in the rest

            // instead of destructively removing the nodes from NodeHeader table
            // which would be valid but would make mining non-idempotent, we
            // instead store the nodes to skip in a separate set

            Set<Integer> alreadyMinedItems = new HashSet<>();
            for (FPTreeNode node : singlePathNodes) {
                alreadyMinedItems.add(node.getItem());
            }

            for (Map.Entry<Integer, FPTreeNode> header : nodeHeaders.entrySet()) {
                if (alreadyMinedItems.contains(header.getKey())) {
                    continue;
                }

                // add the singleton item set
                branchingItemsets.add(new ItemsetWithCount(Sets.newHashSet(header.getKey()),
                                                           frequentItemCounts.get(header.getKey())));

                List<ItemsetWithCount> conditionalPatternBase = new ArrayList<>();

                // walk each "leaf" node
                FPTreeNode conditionalNode = header.getValue();
                while (conditionalNode != null) {
                    final double leafSupport = conditionalNode.getCount();

                    // walk the tree up to the branch node
                    Set<Integer> conditionalPattern = new HashSet<>();
                    FPTreeNode walkNode = conditionalNode.getParent();
                    while (walkNode != nodeOfBranching.getParent() && walkNode != root) {
                        conditionalPattern.add(walkNode.getItem());
                        walkNode = walkNode.getParent();
                    }

                    if (conditionalPattern.size() > 0) {
                        conditionalPatternBase.add(new ItemsetWithCount(conditionalPattern, leafSupport));
                    }

                    conditionalNode = conditionalNode.getNextLink();
                }

                if (conditionalPatternBase.isEmpty()) {
                    continue;
                }

                // build and mine the conditional FPTree
                FPTree conditionalTree = new FPTree();
                conditionalTree.insertConditionalFrequentItems(conditionalPatternBase, supportCountRequired);
                conditionalTree.insertConditionalFrequentPatterns(conditionalPatternBase);
                List<ItemsetWithCount> conditionalFrequentItemsets = conditionalTree.mineItemsets(supportCountRequired);

                if (!conditionalFrequentItemsets.isEmpty()) {
                    for (ItemsetWithCount is : conditionalFrequentItemsets) {
                        is.getItems().add(header.getKey());
                    }

                    branchingItemsets.addAll(conditionalFrequentItemsets);
                }
            }

            if (singlePathItemsets.isEmpty()) {
                return branchingItemsets;
            }

            // take the cross product of the mined itemsets
            List<ItemsetWithCount> ret = new ArrayList<>();

            ret.addAll(singlePathItemsets);
            ret.addAll(branchingItemsets);

            for (ItemsetWithCount i : singlePathItemsets) {
                for (ItemsetWithCount j : branchingItemsets) {
                    Set<Integer> combinedItems = new HashSet<>();
                    combinedItems.addAll(i.getItems());
                    combinedItems.addAll(j.getItems());

                    ret.add(new ItemsetWithCount(combinedItems, Math.min(i.getCount(), j.getCount())));
                }
            }

            return ret;
        }
    }


    public List<ItemsetWithCount> getItemsetsWithSupportRatio(List<Set<Integer>> transactions,
                                                              Double supportRatio) {
        return getItemsetsWithSupportRatio(transactions, null, supportRatio);
    }

    public List<ItemsetWithCount> getItemsetsWithSupportRatio(List<Set<Integer>> transactions,
                                                              Map<Integer, Double> initialCounts,
                                                              Double supportRatio) {
        return getItemsetsWithSupportCount(transactions, initialCounts, supportRatio * transactions.size());
    }

    public List<ItemsetWithCount> getItemsetsWithSupportCount(List<Set<Integer>> transactions,
                                                              Double supportCount) {
        return getItemsetsWithSupportCount(transactions, null, supportCount);

    }

    public List<ItemsetWithCount> getItemsetsWithSupportCount(List<Set<Integer>> transactions,
                                                              Map<Integer, Double> initialCounts,
                                                              Double supportCount) {
        return getItemsetsWithSupportCount(transactions, initialCounts, supportCount, false);
    }

    protected FPTree constructTree(List<Set<Integer>> transactions, int supportCount) {
        FPTree fp = new FPTree();
        fp.insertFrequentItems(transactions, supportCount);
        fp.insertTransactions(transactions);
        return fp;
    }

    public List<ItemsetWithCount> getItemsetsWithSupportCount(List<Set<Integer>> transactions,
                                                              Map<Integer, Double> initialCounts,
                                                              Double supportCount,
                                                              boolean printTreeDebug) {
        FPTree fp = new FPTree();
        int countRequiredForSupport = supportCount.intValue();
        log.debug("count required: {}", countRequiredForSupport);

        long st = System.currentTimeMillis();

        Timer.Context context = singleItemCounts.time();
        if (initialCounts == null) {
            fp.insertFrequentItems(transactions, countRequiredForSupport);
        } else {
            fp.setFrequentCounts(initialCounts);
        }

        fp.insertFrequentItems(transactions, countRequiredForSupport);
        context.stop();
        context = insertTransactions.time();
        fp.insertTransactions(transactions);
        context.stop();
        long en = System.currentTimeMillis();

        log.debug("FPTree load: {}", en - st);

        //fp.printTreeDebug();

        st = System.currentTimeMillis();
        context = fpMine.time();
        List<ItemsetWithCount> ret = fp.mineItemsets(countRequiredForSupport);
        context.stop();
        en = System.currentTimeMillis();

        log.debug("FPTree mine: {}", en - st);

        return ret;
    }

    // ugh, this is a really ugly function sig, but it's efficient
    public List<ItemsetWithCount> getCounts(
            List<Datum> transactions,
            Map<Integer, Double> initialCounts,
            Set<Integer> targetItems,
            List<ItemsetWithCount> toCount) {
        FPTree countTree = new FPTree();

        Map<Integer, Double> frequentCounts = new HashMap<>();

        for (Integer i : targetItems) {
            Double initialCount = initialCounts.get(i);
            if (initialCount == null) {
                initialCount = 0.;
            }
            frequentCounts.put(i, initialCount);
        }

        countTree.setFrequentCounts(frequentCounts);
        countTree.insertDatum(transactions);

        List<ItemsetWithCount> ret = new ArrayList<>();
        for (ItemsetWithCount c : toCount) {
            ret.add(new ItemsetWithCount(c.getItems(), countTree.getSupport(c.getItems())));
        }

        return ret;
    }
}

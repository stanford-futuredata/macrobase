package macrobase.analysis.summary.itemset;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import macrobase.MacroBase;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class StreamingFPGrowth {
    private static final Logger log = LoggerFactory.getLogger(StreamingFPGrowth.class);
    private final Timer fpMine = MacroBase.metrics.timer(name(StreamingFPGrowth.class, "fpMine"));
    private final Timer restructureTree = MacroBase.metrics.timer(name(StreamingFPGrowth.class, "restructureTree"));
    private final Timer updateFrequentItemOrder = MacroBase.metrics.timer(
            name(StreamingFPGrowth.class, "updateFrequentItemOrder"));
    private final Timer insertFrequentItems = MacroBase.metrics.timer(
            name(StreamingFPGrowth.class, "insertFrequentItems"));

    StreamingFPTree fp = new StreamingFPTree();
    boolean needsRestructure = false;
    boolean startedStreaming = false;


    private final double support;

    public StreamingFPGrowth(double support) {
        this.support = support;
    }

    class StreamingFPTree {
        private FPTreeNode root = new FPTreeNode(-1, null, 0);
        // used to calculate the order
        private Map<Integer, Double> frequentItemCounts = new HashMap<>();

        // item order -- need canonical to break ties; 0 is smallest, N is largest
        private Map<Integer, Integer> frequentItemOrder = new HashMap<>();

        protected Map<Integer, FPTreeNode> nodeHeaders = new HashMap<>();

        protected Set<FPTreeNode> leafNodes = new HashSet<>();

        Set<FPTreeNode> sortedNodes = new HashSet<>();

        private void printTreeDebug() {
            log.debug("Frequent Item Counts:");
            frequentItemCounts.entrySet().forEach(e -> log.debug("{} {}", e.getKey(), e.getValue()));

            log.debug("Frequent Item Order:");
            frequentItemOrder.entrySet().forEach(
                    e -> log.debug("{} {}", e.getKey(), e.getValue()));


            walkTree(root, 1);
        }

        // todo: make more efficient
        private void decayWeights(FPTreeNode start, double decayWeight) {
            if (start == root) {
                for (Integer item : frequentItemCounts.keySet()) {
                    frequentItemCounts.put(item, frequentItemCounts.get(item) * decayWeight);
                }
            }

            start.count *= decayWeight;
            if (start.getChildren() != null) {
                for (FPTreeNode child : start.getChildren()) {
                    decayWeights(child, decayWeight);
                }
            }
        }


        private void walkTree(FPTreeNode start, int treeDepth) {
            log.debug("{} node: {}, count: {}, sorted: {}",
                      new String(new char[treeDepth]).replaceAll("\0", "\t"),
                      start.getItem(), start.getCount(),
                      sortedNodes.contains(start));
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
            private FPTreeNode prevLink;
            private FPTreeNode parent;
            private List<FPTreeNode> children;

            public FPTreeNode(int item, FPTreeNode parent, double initialCount) {
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


            public void decrementCount(double by) {
                count -= by;
            }

            public boolean hasChildren() {
                return children != null && children.size() > 0;
            }

            public void removeChild(FPTreeNode child) {
                assert (children.contains(child));
                children.remove(child);
            }

            public void setNextLink(FPTreeNode nextLink) {
                this.nextLink = nextLink;
            }

            public FPTreeNode getNextLink() {
                return nextLink;
            }

            public void setPrevLink(FPTreeNode prevLink) {
                this.prevLink = prevLink;
            }

            public FPTreeNode getPrevLink() {
                return prevLink;
            }

            public FPTreeNode getParent() {
                return parent;
            }


            public List<FPTreeNode> getChildren() {
                return children;
            }

            public void mergeChildren(List<FPTreeNode> otherChildren) {
                assert (!hasChildren() || !leafNodes.contains(this));

                if (otherChildren == null) {
                    return;
                }

                if (children == null) {
                    children = Lists.newArrayList(otherChildren);
                    for (FPTreeNode child : otherChildren) {
                        child.parent = this;
                    }
                    leafNodes.remove(this);

                    return;
                }

                // O(N^2); slow for large lists; consider optimizing
                for (FPTreeNode otherChild : otherChildren) {
                    otherChild.parent = this;
                    boolean matched = false;
                    for (FPTreeNode ourChild : children) {
                        if (otherChild.item == ourChild.item) {
                            removeNodeFromHeaders(otherChild);

                            ourChild.count += otherChild.count;
                            ourChild.mergeChildren(otherChild.getChildren());

                            matched = true;
                            break;
                        }
                    }

                    if (!matched) {
                        children.add(otherChild);
                    }
                }
            }

            // insert the transaction at this node starting with transaction[currentIndex]
            // then find the child that matches
            public void insertTransaction(List<Integer> fullTransaction,
                                          int currentIndex,
                                          final double itemCount,
                                          boolean streaming) {
                if (!streaming) {
                    sortedNodes.add(this);
                }

                incrementCount(itemCount);

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

                    if (!streaming) {
                        sortedNodes.add(matchingChild);
                    }

                    FPTreeNode prevHeader = nodeHeaders.get(currentItem);
                    nodeHeaders.put(currentItem, matchingChild);

                    if (prevHeader != null) {
                        matchingChild.setNextLink(prevHeader);
                        prevHeader.setPrevLink(matchingChild);
                    }

                    if (children == null) {
                        children = new ArrayList<>();
                    }

                    children.add(matchingChild);

                    if (currentIndex == fullTransaction.size() - 1) {
                        leafNodes.add(matchingChild);
                    }

                    leafNodes.remove(this);
                }

                matchingChild.insertTransaction(fullTransaction, currentIndex + 1, itemCount, streaming);
            }
        }

        public int getSupport(Collection<Integer> pattern) {
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

        public void insertFrequentItems(List<Set<Integer>> transactions,
                                        int countRequiredForSupport) {
            Timer.Context context = insertFrequentItems.time();

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

            // we have to materialize a canonical order so that items with equal counts
            // are consistently ordered when they are sorted during transaction insertion
            List<Map.Entry<Integer, Double>> sortedItemCounts = Lists.newArrayList(frequentItemCounts.entrySet());
            sortedItemCounts.sort((i1, i2) -> frequentItemCounts.get(i1.getKey())
                    .compareTo(frequentItemCounts.get(i2.getKey())));
            for (int i = 0; i < sortedItemCounts.size(); ++i) {
                frequentItemOrder.put(sortedItemCounts.get(i).getKey(), i);
            }

            context.stop();
        }

        private void deleteItems(Set<Integer> itemsToDelete) {
            if (itemsToDelete == null) {
                return;
            }

            for (int item : itemsToDelete) {
                frequentItemCounts.remove(item);
                frequentItemOrder.remove(item);

                FPTreeNode nodeToDelete = nodeHeaders.get(item);

                while (nodeToDelete != null) {
                    nodeToDelete.parent.removeChild(nodeToDelete);
                    if (nodeToDelete.hasChildren()) {
                        nodeToDelete.parent.mergeChildren(nodeToDelete.children);
                    }

                    leafNodes.remove(nodeToDelete);

                    nodeToDelete = nodeToDelete.getNextLink();
                }

                nodeHeaders.remove(item);
            }
        }

        private void updateFrequentItemOrder() {
            Timer.Context context = updateFrequentItemOrder.time();

            sortedNodes.clear();

            frequentItemOrder.clear();

            // we have to materialize a canonical order so that items with equal counts
            // are consistently ordered when they are sorted during transaction insertion
            List<Map.Entry<Integer, Double>> sortedItemCounts = Lists.newArrayList(frequentItemCounts.entrySet());
            sortedItemCounts.sort((i1, i2) -> frequentItemCounts.get(i1.getKey())
                    .compareTo(frequentItemCounts.get(i2.getKey())));
            for (int i = 0; i < sortedItemCounts.size(); ++i) {
                frequentItemOrder.put(sortedItemCounts.get(i).getKey(), i);
            }

            context.stop();
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

            updateFrequentItemOrder();
        }

        private void sortTransaction(List<Integer> txn, boolean isStreaming) {
            if (!isStreaming) {
                txn.sort((i1, i2) -> frequentItemOrder.get(i2).compareTo(frequentItemOrder.get(i1)));
            } else {
                txn.sort((i1, i2) ->
                                 frequentItemOrder.compute(i2, (k, v) -> v == null ? -i2 : v)
                                         .compareTo(frequentItemOrder.compute(i1, (k, v) -> v == null ? -i1 : v)));
            }
        }

        public void insertConditionalFrequentPatterns(List<ItemsetWithCount> patterns) {
            for (ItemsetWithCount is : patterns) {
                reinsertBranch(is.getItems(), is.getCount(), root);
            }
        }

        public void reinsertBranch(Set<Integer> pattern, double count, FPTreeNode rootOfBranch) {
            List<Integer> filtered = pattern.stream().filter(i -> frequentItemCounts.containsKey(i)).collect(
                    Collectors.toList());
            sortTransaction(filtered, false);
            rootOfBranch.insertTransaction(filtered, 0, count, false);
        }


        public void insertTransactions(List<Set<Integer>> transactions, boolean streaming, boolean filterExistingFrequentItemsOnly) {
            for (Set<Integer> t : transactions) {
                insertTransaction(t, streaming, filterExistingFrequentItemsOnly);
            }
        }

        public void insertTransaction(Collection<Integer> transaction, boolean streaming, boolean filterExistingFrequentItemsOnly) {
            if (streaming && !filterExistingFrequentItemsOnly) {
                for (Integer item : transaction) {
                    frequentItemCounts.compute(item, (k, v) -> v == null ? 1 : v + 1);
                }
            }

            List<Integer> filtered = transaction.stream().filter(i -> frequentItemCounts.containsKey(i)).collect(
                    Collectors.toList());

            if (!filtered.isEmpty()) {
                if (streaming && filterExistingFrequentItemsOnly) {
                    for (Integer item : filtered) {
                        frequentItemCounts.compute(item, (k, v) -> v == null ? 1 : v + 1);
                    }
                }

                sortTransaction(filtered, streaming);
                root.insertTransaction(filtered, 0, 1, streaming);
            }
        }

        List<ItemsetWithCount> mineItemsets(Integer supportCountRequired) {
            List<ItemsetWithCount> singlePathItemsets = new ArrayList<>();
            List<ItemsetWithCount> branchingItemsets = new ArrayList<>();

            // mine single-path itemsets first
            FPTreeNode curNode = root;
            FPTreeNode nodeOfBranching = null;
            Set<FPTreeNode> singlePathNodes = new HashSet<>();
            while (true) {
                if (curNode.count < supportCountRequired) {
                    break;
                }

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
            // due to the descending frequency count of the StreamingFPTree structure, so
            // we remove them from consideration in the rest

            // instead of destructively removing the nodes from NodeHeader table
            // which would be valid but would make mining non-idempotent, we
            // instead store the nodes to skip in a separate set

            Set<Integer> alreadyMinedItems = new HashSet<>();
            for (FPTreeNode node : singlePathNodes) {
                alreadyMinedItems.add(node.getItem());
            }

            for (Map.Entry<Integer, FPTreeNode> header : nodeHeaders.entrySet()) {
                if (alreadyMinedItems.contains(header.getKey())
                    || frequentItemCounts.get(header.getKey()) < supportCountRequired) {
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

                // build and mine the conditional StreamingFPTree
                StreamingFPTree conditionalTree = new StreamingFPTree();
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

        private void removeNodeFromHeaders(FPTreeNode node) {
            leafNodes.remove(node);

            if (node.getPrevLink() == null) {
                assert (nodeHeaders.get(node.getItem()) == node);
                nodeHeaders.put(node.getItem(), node.getNextLink());
            } else {
                node.getPrevLink().setNextLink(node.getNextLink());
            }

            if (node.getNextLink() != null) {
                node.getNextLink().setPrevLink(node.getPrevLink());
            }
        }

        private void sortByNewOrder() {
            // we need to walk the tree from each leaf to each root

            List<FPTreeNode> leavesToInspect = Lists.newArrayList(leafNodes);
            Set<FPTreeNode> removedNodes = new HashSet<>();
            for (int i = 0; i < leavesToInspect.size(); ++i) {
                FPTreeNode leaf = leavesToInspect.get(i);

                if (leaf == root) {
                    continue;
                }

                if (removedNodes.contains(leaf) || sortedNodes.contains(leaf)) {
                    continue;
                }

                double leafCount = leaf.getCount();
                Set<Integer> toInsert = new HashSet<>();

                toInsert.add(leaf.getItem());

                assert (!leaf.hasChildren());

                removeNodeFromHeaders(leaf);

                removedNodes.add(leaf);

                int curLowestNodeOrder = frequentItemOrder.get(leaf.getItem());

                FPTreeNode node = leaf.getParent();
                node.removeChild(leaf);
                while (true) {
                    if (node == root) {
                        break;
                    }

                    int nodeOrder = frequentItemOrder.get(node.getItem());
                    if (sortedNodes.contains(node) && nodeOrder < curLowestNodeOrder) {
                        break;
                    } else if (nodeOrder < curLowestNodeOrder) {
                        curLowestNodeOrder = nodeOrder;
                    }

                    assert (!removedNodes.contains(node));

                    toInsert.add(node.getItem());

                    node.decrementCount(leafCount);
                    // this node no longer has support, so remove it...
                    if (node.getCount() == 0 && !node.hasChildren()) {
                        removedNodes.add(node);
                        removeNodeFromHeaders(node);
                        node.getParent().removeChild(node);
                        // still has support but is unsorted, so we'd better check it out
                    } else if (!node.hasChildren() && !sortedNodes.contains(node)) {
                        leavesToInspect.add(node);
                    }

                    node = node.getParent();
                }

                node.decrementCount(leafCount);

                reinsertBranch(toInsert, leafCount, node);
            }
        }
    }

    public void insertTransactionsStreamingExact(List<Set<Integer>> transactions) {
        needsRestructure = true;

        fp.insertTransactions(transactions, true, false);
    }

    public void insertTransactionStreamingExact(Collection<Integer> transaction) {
        needsRestructure = true;

        fp.insertTransaction(transaction, true, false);
    }

    public void insertTransactionsStreamingFalseNegative(List<Set<Integer>> transactions) {
        needsRestructure = true;

        fp.insertTransactions(transactions, true, true);
    }

    public void insertTransactionStreamingFalseNegative(Collection<Integer> transaction) {
        needsRestructure = true;

        fp.insertTransaction(transaction, true, true);
    }

    public void restructureTree(Set<Integer> itemsToDelete) {
        needsRestructure = false;
        // todo: prune infrequent items
        Timer.Context context = restructureTree.time();

        fp.deleteItems(itemsToDelete);
        fp.updateFrequentItemOrder();
        fp.sortByNewOrder();

        context.stop();
    }

    public void buildTree(List<Set<Integer>> transactions) {
        if (startedStreaming) {
            throw new RuntimeException("Can't build a tree based on an already streaming tree...");
        }

        int countRequiredForSupport = (int) (support * transactions.size());

        fp.insertFrequentItems(transactions, countRequiredForSupport);
        fp.insertTransactions(transactions, false, false);
    }

    public void decayAndResetFrequentItems(Map<Integer, Double> newFrequentItems, double decayRate) {
        Set<Integer> toRemove = Sets.difference(fp.frequentItemOrder.keySet(),
                                                newFrequentItems.keySet()).immutableCopy();
        fp.frequentItemCounts = newFrequentItems;
        fp.updateFrequentItemOrder();
        if (decayRate > 0) {
            fp.decayWeights(fp.root, (1 - decayRate));
        }
        restructureTree(toRemove);
    }

    public List<ItemsetWithCount> getCounts(List<ItemsetWithCount> targets) {
        if(needsRestructure){
            restructureTree(null);
        }

        List<ItemsetWithCount> ret = new ArrayList<>(targets.size());
        for (ItemsetWithCount target : targets) {
            ret.add(new ItemsetWithCount(target.getItems(), fp.getSupport(target.getItems())));
        }
        return ret;
    }

    public List<ItemsetWithCount> getItemsets() {
        if (needsRestructure) {
            restructureTree(null);
        }

        Timer.Context context = fpMine.time();
        List<ItemsetWithCount> itemset = fp.mineItemsets((int) (fp.root.getCount() * support));
        context.stop();

        return itemset;
    }

    public void printTreeDebug() {
        fp.printTreeDebug();
    }
}

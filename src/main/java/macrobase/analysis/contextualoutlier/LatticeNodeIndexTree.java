package macrobase.analysis.contextualoutlier;

import java.util.*;

public class LatticeNodeIndexTree {

    private Map<Integer, LatticeNodeIndexTree> dimension2SubTree = new HashMap<Integer, LatticeNodeIndexTree>();
    private LatticeNode left; //the context this tree represents if  dimension2SubTree is empty
    
    /**
     * Add a to the tree
     * @param c
     */
    public void addLatticeNode(LatticeNode c) {
        List<Integer> dimensions = c.getDimensions();
        addLatticeNode(c, dimensions);
    }
    
    private void addLatticeNode(LatticeNode c, List<Integer> dimensions) {
        Integer dimension = dimensions.get(0);
        if (dimension2SubTree.containsKey(dimension)) {
        } else {
            LatticeNodeIndexTree cit = new LatticeNodeIndexTree();
            dimension2SubTree.put(dimension, cit);
        }
        if (dimensions.size() == 1) {
            dimension2SubTree.get(dimension).left = c;
            return;
        }
        dimension2SubTree.get(dimension).addLatticeNode(c, dimensions.subList(1, dimensions.size()));
        
    }
    
    public LatticeNode getLatticeNode(List<Integer> dimensions) {
        if (!dimension2SubTree.containsKey(dimensions.get(0))) {
            return null;
        }
        
        if (dimensions.size() == 1) {
            return dimension2SubTree.get(dimensions.get(0)).left;
        }
        
        LatticeNodeIndexTree subTree = dimension2SubTree.get(dimensions.get(0));
        return subTree.getLatticeNode(dimensions.subList(1, dimensions.size()));
    }

}

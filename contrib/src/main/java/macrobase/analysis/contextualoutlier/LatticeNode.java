package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import macrobase.datamodel.Datum;

public class LatticeNode {

    List<Integer> dimensions; // A list of ordered contextual dimensions this node represents;
    List<Context> denseContexts; //A list of dense contexts this node contains

    public LatticeNode(int dimension) {
        dimensions = new ArrayList<Integer>();
        dimensions.add(dimension);
        denseContexts = new ArrayList<Context>();
    }

    public LatticeNode(List<Integer> dimensions) {
        this.dimensions = dimensions;
        denseContexts = new ArrayList<Context>();
    }

    public List<Context> getDenseContexts() {
        return denseContexts;
    }

    public void addDenseContext(Context c) {
        denseContexts.add(c);
    }

    public LatticeNode join(LatticeNode other, List<ContextualDatum> data, double tau) {
        List<Integer> newDimensions = joinedDimensions(other);
        if (newDimensions == null)
            return null;
        //now start to join the dense units
        LatticeNode result = new LatticeNode(newDimensions);
        for (Context u1 : getDenseContexts()) {
            for (Context u2 : other.getDenseContexts()) {
                Context newUnit = u1.join(u2, data, tau);
                if (newUnit != null)
                    result.addDenseContext(newUnit);
            }
        }
        return result;
    }

    public List<Integer> getDimensions() {
        return dimensions;
    }
    
    /**
     * Joins this subspace with the specified subspace. The join is only
     * successful if both subspaces have the first k-1 dimensions in common (where
     * k is the number of dimensions).
     * <p>
     * Return null is not successful
     *
     * @param other
     * @return
     */
    public List<Integer> joinedDimensions(LatticeNode other) {
        //check the dimensions first
        if (dimensions.size() != other.dimensions.size())
            return null;
        List<Integer> newDimensions = new ArrayList<Integer>();
        for (int i = 0; i < dimensions.size() - 1; i++) {
            if (dimensions.get(i) != other.dimensions.get(i))
                return null;
            else
                newDimensions.add(dimensions.get(i));
        }
        int lastDimension1 = dimensions.get(dimensions.size() - 1);
        int lastDimension2 = other.dimensions.get(dimensions.size() - 1);
        if (lastDimension1 == lastDimension2) {
            return null;
        } else if (lastDimension1 < lastDimension2) {
            newDimensions.add(lastDimension1);
            newDimensions.add(lastDimension2);
        } else {
            newDimensions.add(lastDimension2);
            newDimensions.add(lastDimension1);
        }
        return newDimensions;
    }

    /**
     * Compare the subspace based on the dimensions
     *
     * @author xuchu
     */
    public static class DimensionComparator implements Comparator<LatticeNode> {
        @Override
        public int compare(LatticeNode s1, LatticeNode s2) {
            if (s1 == s2) {
                return 0;
            }
            if (s1.dimensions == null && s2.dimensions != null) {
                return -1;
            }
            if (s1.dimensions != null && s2.dimensions == null) {
                return 1;
            }
            int compare = s1.dimensions.size() - s2.dimensions.size();
            if (compare != 0) {
                return compare;
            }
            for (int i = 0; i < s1.dimensions.size(); i++) {
                int d1 = s1.dimensions.get(i);
                int d2 = s2.dimensions.get(i);
                if (d1 != d2) {
                    return d1 - d2;
                }
            }
            return 0;
        }
    }

    public void clear() {
        dimensions.clear();
        denseContexts.clear();
    }
}

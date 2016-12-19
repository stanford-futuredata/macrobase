package macrobase.analysis.stats.kde.grid;

import macrobase.analysis.stats.kde.kernel.Kernel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Performs cutoffs on unscaled densities by tracking minimum total densities
 * inside rectangular regions of space.
 */
public class SoftGridCutoff {
    public Kernel kernel;
    public double[] gridSize;
    public double cutoff;
    public boolean ignoreSelf = false;

    // Useful to have a sparse grid here, though map is expensive
    public Map<IntVec, Double> grid;
    public double densityOfSinglePoint;
    // Only consider adjacent neighbors with one differing component
    public double[] densityOfNeighbor;

    public double[] offset;

    /**
     * Fast class to use int[] as keys into our sparse grid map
     */
    final class IntVec {
        int[] vec;
        public IntVec(int[] v) {
            this.vec = v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            IntVec intVec = (IntVec) o;
            for (int i = 0; i < vec.length; i++) {
                if (vec[i] != intVec.vec[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(vec);
        }

        @Override public String toString() {
            return Arrays.toString(vec);
        }
    }

    public SoftGridCutoff(Kernel k, double[] s, double c) {
        this.kernel = k;
        this.gridSize = s.clone();
        this.cutoff = c;

        this.offset = new double[gridSize.length];
        this.grid = new HashMap<>();

        this.densityOfSinglePoint = k.density(gridSize);
        this.densityOfNeighbor = new double[gridSize.length];
        for (int i=0; i < gridSize.length; i++) {
            double[] nDist = gridSize.clone();
            nDist[i] = gridSize[i]*2;
            this.densityOfNeighbor[i] = k.density(nDist);
        }
    }

    public SoftGridCutoff ignoreSelf(boolean f) {
        this.ignoreSelf = f;
        return this;
    }

    public SoftGridCutoff setOffset(double f) {
        for (int i = 0; i < gridSize.length; i++) {
            offset[i] = gridSize[i] * f;
        }
        return this;
    }

    private IntVec transformToKey(double[] p) {
        int[] p2 = new int[p.length];
        for (int i = 0; i < p.length; i++) {
            double tVal = (p[i] / gridSize[i]) + offset[i];
            if (tVal >= 0) {
                p2[i] = (int)tVal;
            } else {
                p2[i] = (int)(tVal - 1);
            }
        }
        return new IntVec(p2);
    }

    public void add(double[] p) {
        IntVec k = transformToKey(p);

        double curWeight = grid.getOrDefault(k, 0.0);
        grid.put(k, curWeight + densityOfSinglePoint);

        for (int i = 0; i < p.length; i++) {
            for (int di=-1; di<=1; di+=2) {
                int[] kNeighborArray = k.vec.clone();
                kNeighborArray[i] += di;
                IntVec kNeighbor = new IntVec(kNeighborArray);
                grid.put(kNeighbor,
                        grid.getOrDefault(kNeighbor, 0.0) + densityOfNeighbor[i]
                );
            }
        }

    }

    public SoftGridCutoff prune() {
        if (ignoreSelf) {
            double ignoreSelfCutoff = cutoff + densityOfSinglePoint;
            grid.entrySet().removeIf(e -> e.getValue() < ignoreSelfCutoff);
        } else {
            grid.entrySet().removeIf(e -> e.getValue() < cutoff);
        }
        return this;
    }

    public int getNumCells() {
        return grid.size();
    }

    public double getUnscaledDenseValue(double[] p) {
        IntVec k = transformToKey(p);
        double weight = grid.getOrDefault(k, 0.0);
        if (weight > 0) {
            if (ignoreSelf) {
                return weight - densityOfSinglePoint;
            } else {
                return weight;
            }
        } else {
            return 0.0;
        }
    }
}

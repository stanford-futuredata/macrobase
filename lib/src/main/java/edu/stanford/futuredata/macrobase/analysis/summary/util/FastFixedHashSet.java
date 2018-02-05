package edu.stanford.futuredata.macrobase.analysis.summary.util;

/**
 * A HashSet for IntSetAsLongs.  Requires that all keys be nonzero.
 * Does not resize.
 */
public class FastFixedHashSet {
    private IntSet hashSet[];
    private long hashLongSet[];
    private int mask;
    private int capacity;
    private boolean useIntArraySets;

    public FastFixedHashSet(int size, boolean useIntArraySets) {
        int realSize = 1;
        while(realSize < size) {
            realSize *= 2;
        }
        this.capacity = realSize;
        this.mask = realSize - 1;
        if (useIntArraySets)
            hashSet = new IntSet[realSize];
        else
            hashLongSet = new long[realSize];
        this.useIntArraySets = useIntArraySets;
    }

    public void add(IntSet entry) {
        int hashed = entry.hashCode();
        int index = (hashed) & mask;
        while(hashSet[index] != null) {
            index = (index + 1) & mask;
        }
        hashSet[index] = entry;
    }

    public void add(long entry) {
        int hashed = (int) ((entry + 31 * (entry >>> 11)  + 31 * (entry >>> 22) + 7 * (entry >>> 31)
                + (entry >>> 45) + 31 * (entry >>> 7) + 7 * (entry >>> 37)));
        int index = (hashed) & mask;
        while(hashLongSet[index] != 0) {
            index = (index + 1) & mask;
        }
        hashLongSet[index] = entry;
    }

    public boolean contains (IntSet entry) {
        if (useIntArraySets) {
            int hashed = entry.hashCode();
            int index = (hashed) & mask;
            while (hashSet[index] != null && !(hashSet[index].equals(entry))) {
                index = (index + 1) & mask;
            }
            return (hashSet[index] != null);
        } else {
            long realEntry = ((IntSetAsLong) entry).value;
            int hashed = entry.hashCode();
            int index = (hashed) & mask;
            while (hashLongSet[index] != 0 && !(hashLongSet[index] == realEntry)) {
                index = (index + 1) & mask;
            }
            return (hashLongSet[index] != 0);
        }
    }

    public int getCapacity() {
        return capacity;
    }
}

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

    public FastFixedHashSet(int size, boolean useIntSet) {
        int realSize = 1;
        while(realSize < size) {
            realSize *= 2;
        }
        this.capacity = realSize;
        this.mask = realSize - 1;
        if (useIntSet)
            hashSet = new IntSet[realSize];
        else
            hashLongSet = new long[realSize];
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
        int hashed = entry.hashCode();
        int index = (hashed) & mask;
        while(hashSet[index] != null && !(hashSet[index].equals(entry))) {
            index = (index + 1) & mask;
        }
        return (hashSet[index] != null);
    }

    public boolean contains (long entry) {
        int hashed = (int) ((entry + 31 * (entry >>> 11)  + 31 * (entry >>> 22) + 7 * (entry >>> 31)
                + (entry >>> 45) + 31 * (entry >>> 7) + 7 * (entry >>> 37)));
        int index = (hashed) & mask;
        while(hashLongSet[index] != 0 && !(hashLongSet[index] == entry)) {
            index = (index + 1) & mask;
        }
        return (hashLongSet[index] != 0);
    }

    public int getCapacity() {
        return capacity;
    }
}

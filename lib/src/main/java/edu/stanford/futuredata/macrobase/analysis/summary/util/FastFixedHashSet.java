package edu.stanford.futuredata.macrobase.analysis.summary.util;

/**
 * A HashSet for IntSetAsLongs.  Requires that all keys be nonzero.
 * Does not resize.
 */
public class FastFixedHashSet {
    private IntSet hashSet[];
    private int mask;
    private int capacity;

    public FastFixedHashSet(int size) {
        int realSize = 1;
        while(realSize < size) {
            realSize *= 2;
        }
        this.capacity = realSize;
        this.mask = realSize - 1;
        hashSet = new IntSet[realSize];
    }

    public void add(IntSet entry) {
        int hashed = entry.hashCode();
        int index = (hashed) & mask;
        while(hashSet[index] != null) {
            index = (index + 1) & mask;
        }
        hashSet[index] = entry;
    }

    public boolean contains (IntSet entry) {
        int hashed = entry.hashCode();
        int index = (hashed) & mask;
        while(hashSet[index] != null && !(hashSet[index].equals(entry))) {
            index = (index + 1) & mask;
        }
        return (hashSet[index] != null);
    }

    public int getCapacity() {
        return capacity;
    }
}

package edu.stanford.futuredata.macrobase.analysis.summary.util;

public class FastFixedHashSet {
    private long hashSet[];
    private int mask;
    private int capacity;

    public FastFixedHashSet(int size) {
        int realSize = 1;
        while(realSize < size) {
            realSize *= 2;
        }
        this.capacity = realSize;
        this.mask = realSize - 1;
        hashSet = new long[realSize];
    }

    public void add(long entry) {
        long hashed = entry + (entry >>> 11)  + (entry >>> 22) + (entry >>> 31) + (entry >>> 45) + (entry >>> 7) + (entry >>> 37);
        int index = ((int) hashed) & mask;
        while(hashSet[index] != 0) {
            index = (index + 1) & mask;
        }
        hashSet[index] = entry;
    }

    public boolean contains (long entry) {
        long hashed = entry + (entry >>> 11)  + (entry >>> 22) + (entry >>> 31) + (entry >>> 45) + (entry >>> 7) + (entry >>> 37);
        int index = ((int) hashed) & mask;
        while(hashSet[index] != 0 && hashSet[index] != entry) {
            index = (index + 1) & mask;
        }
        return (hashSet[index] != 0);
    }
}

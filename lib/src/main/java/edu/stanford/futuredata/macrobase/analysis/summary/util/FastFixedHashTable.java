package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.ArrayList;
import java.util.List;

/**
 * A HashTable from IntSets to arrays of doubles.  Requires that all keys
 * be nonzero.
 */
public class FastFixedHashTable {
    private double hashTable[][];
    private IntSet existsTable[];
    private long existsLongTable[];
    private int numAggregates;
    private int mask;
    private int capacity;
    private boolean useIntArraySets;
    private int size = 0;
    private final int ratio = 10;

    public FastFixedHashTable(int size, int numAggregates, boolean useIntArraySets) {
        int realSize = 1;
        while(realSize < size) {
            realSize *= 2;
        }
        this.capacity = realSize;
        this.mask = realSize - 1;
        this.numAggregates = numAggregates;
        hashTable = new double[realSize][numAggregates];
        if (useIntArraySets)
            existsTable = new IntSet[realSize];
        else
            existsLongTable = new long[realSize];
        this.useIntArraySets = useIntArraySets;
    }

    private void growAndRehash() {
        this.size = 1;
        int oldCapacity = capacity;
        this.capacity = capacity * 2;
        this.mask = capacity - 1;
        double [][] oldHashTable = this.hashTable;
        this.hashTable = new double[capacity][numAggregates];
        if (useIntArraySets) {
            IntSet[] oldExistsTable = this.existsTable;
            this.existsTable = new IntSet[capacity];
            for(int i = 0; i < oldCapacity; i++) {
                if (oldExistsTable[i] != null) {
                    put(oldExistsTable[i], oldHashTable[i]);
                }
            }
        } else {
            long[] oldExistsLongTable = this.existsLongTable;
            this.existsLongTable = new long[capacity];
            for (int i = 0; i < oldCapacity; i++) {
                if (oldExistsLongTable[i] != 0) {
                    put(oldExistsLongTable[i], oldHashTable[i]);
                }
            }
        }
    }

    public void put(IntSet entry, double[] aggregates) {
        size++;
        if (size * ratio > capacity)
            growAndRehash();
        if (useIntArraySets) {
            int hashed = entry.hashCode();
            int index = (hashed) & mask;
            while (existsTable[index] != null) {
                index = (index + 1) & mask;
            }
            existsTable[index] = entry;
            for (int i = 0; i < numAggregates; i++) {
                hashTable[index][i] = aggregates[i];
            }
        } else {
            long realEntry = ((IntSetAsLong) entry).value;
            int hashed = entry.hashCode();
            int index = (hashed) & mask;
            while(existsLongTable[index] != 0) {
                index = (index + 1) & mask;
            }
            existsLongTable[index] = realEntry;
            for(int i = 0; i < numAggregates; i++) {
                hashTable[index][i] = aggregates[i];
            }
        }
    }

    public void put(long entry, double[] aggregates) {
        size++;
        if (size * ratio > capacity)
            growAndRehash();
        int hashed = (int) ((entry + 31 * (entry >>> 11)  + 31 * (entry >>> 22) + 7 * (entry >>> 31)
                + (entry >>> 45) + 31 * (entry >>> 7) + 7 * (entry >>> 37)));
        int index = (hashed) & mask;
        while(existsLongTable[index] != 0) {
            index = (index + 1) & mask;
        }
        existsLongTable[index] = entry;
        for(int i = 0; i < numAggregates; i++) {
            hashTable[index][i] = aggregates[i];
        }
    }

    public double[] get(IntSet entry) {
        if (useIntArraySets) {
            int hashed = entry.hashCode();
            int index = (hashed) & mask;
            while (existsTable[index] != null && !(existsTable[index].equals(entry))) {
                index = (index + 1) & mask;
            }
            if (existsTable[index] == null) {
                return null;
            } else {
                return hashTable[index];
            }
        } else {
            long realEntry = ((IntSetAsLong) entry).value;
            int hashed = entry.hashCode();
            int index = (hashed) & mask;
            while(existsLongTable[index] != 0 && !(existsLongTable[index] == realEntry)) {
                index = (index + 1) & mask;
            }
            if(existsLongTable[index] == 0) {
                return null;
            }
            else {
                return hashTable[index];
            }
        }
    }

    public List<IntSet> keySet() {
        ArrayList<IntSet> retList = new ArrayList<>();
        for(int i = 0; i < capacity; i++) {
            if (existsTable[i] != null)
                retList.add(existsTable[i]);
        }
        return retList;
    }

    public List<Long> keySetLong() {
        ArrayList<Long> retList = new ArrayList<>();
        for(int i = 0; i < capacity; i++) {
            if (existsLongTable[i] != 0)
                retList.add(existsLongTable[i]);
        }
        return retList;
    }

    public int getCapacity() {
        return capacity;
    }

}

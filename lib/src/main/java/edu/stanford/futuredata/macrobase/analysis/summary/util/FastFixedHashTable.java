package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.ArrayList;
import java.util.List;

/**
 * A HashTable from IntSetAsLongs to arrays of doubles.  Requires that all keys
 * be nonzero.  Does not resize.
 */
public class FastFixedHashTable {
    private double hashTable[][];
    private IntSet existsTable[];
    private long existsLongTable[];
    private int numAggregates;
    private int mask;
    private int capacity;
    private boolean useIntArraySets;

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

    public void put(IntSet entry, double[] aggregates) {
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

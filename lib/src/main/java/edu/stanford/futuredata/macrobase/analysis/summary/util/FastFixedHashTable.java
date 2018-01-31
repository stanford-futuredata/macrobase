package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.ArrayList;
import java.util.List;

/**
 * A HashTable from IntSetAsLongs to arrays of doubles.  Requires that all keys
 * be nonzero.  Does not resize.
 */
public class FastFixedHashTable {
    private double hashTable[][];
    private long existsTable[];
    private int numAggregates;
    private int mask;
    private int capacity;

    public FastFixedHashTable(int size, int numAggregates) {
        int realSize = 1;
        while(realSize < size) {
            realSize *= 2;
        }
        this.capacity = realSize;
        this.mask = realSize - 1;
        this.numAggregates = numAggregates;
        hashTable = new double[realSize][numAggregates];
        existsTable = new long[realSize];
    }

    public void put(long entry, double[] aggregates) {
        long hashed = entry + 31 * (entry >>> 11)  + 31 * (entry >>> 22) + 7 * (entry >>> 31)
                + (entry >>> 45) + 31 * (entry >>> 7) + 7 * (entry >>> 37);
        int index = ((int) hashed) & mask;
        while(existsTable[index] != 0) {
            index = (index + 1) & mask;
        }
        existsTable[index] = entry;
        for(int i = 0; i < numAggregates; i++) {
            hashTable[index][i] = aggregates[i];
        }
    }

    public double[] get(long entry) {
        long hashed = entry + 31 * (entry >>> 11)  + 31 * (entry >>> 22) + 7 * (entry >>> 31)
                + (entry >>> 45) + 31 * (entry >>> 7) + 7 * (entry >>> 37);
        int index = ((int) hashed) & mask;
        while(existsTable[index] != 0 && existsTable[index] != entry) {
            index = (index + 1) & mask;
        }
        if(existsTable[index] == 0) {
            return null;
        }
        else {
            return hashTable[index];
        }
    }

    public List<Long> keySet() {
        ArrayList<Long> retList = new ArrayList<>();
        for(int i = 0; i < capacity; i++) {
            if (existsTable[i] != 0)
                retList.add(existsTable[i]);
        }
        return retList;
    }

    public int getCapacity() {
        return capacity;
    }

}

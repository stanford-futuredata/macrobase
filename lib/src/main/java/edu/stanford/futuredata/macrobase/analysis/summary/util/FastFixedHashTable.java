package edu.stanford.futuredata.macrobase.analysis.summary.util;

public class FastFixedHashTable {
    private double hashTable[][];
    private int numAggregates;
    private int mask;

    public FastFixedHashTable(int size, int numAggregates) {
        int realSize = 1;
        while(realSize < size) {
            realSize *= 2;
        }
        this.mask = realSize - 1;
        this.numAggregates = numAggregates;
        hashTable = new double[realSize][numAggregates + 1];
    }

    public void put(long entry, double[] aggregates) {
        long hashed = entry + (entry << 20) + (entry << 40);
        int index = ((int) hashed) & mask;
        while(hashTable[index][0] != 0) {
            index = (index + 1) & mask;
        }
        hashTable[index][0] = (double) hashed;
        for (int i = 0; i < numAggregates; i++) {
            hashTable[index][i + 1] = aggregates[i];
        }
    }

    public double[] get(long entry) {
        long hashed = entry + (entry << 20) + (entry << 40);
        int index = ((int) hashed) & mask;
        while(hashTable[index][0] != 0 && hashTable[index][0] != (double) hashed) {
            index = (index + 1) & mask;
        }
        if (hashTable[index][0] == 0) {
            return null;
        }
        else {
            return hashTable[index];
        }
    }

}

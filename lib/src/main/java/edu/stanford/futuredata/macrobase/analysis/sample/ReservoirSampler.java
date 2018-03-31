package edu.stanford.futuredata.macrobase.analysis.sample;

import java.util.Random;

/**
 * Samples using reservoir sampling
 */
public class ReservoirSampler extends Sampler {
    private Random rand;
    private int[] sample;
    private int numProcessed = 0;

    public ReservoirSampler() {
        rand = new Random();
    }

    public ReservoirSampler(int sampleSize) {
        rand = new Random();
        sample = new int[sampleSize];
    }

    /**
     * @param populationSize total size of population being sampled from
     * @param sampleSize size of sample
     */
    public void computeSampleIndices(int populationSize, int sampleSize) {
        sampleIndices = new int[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            sampleIndices[i] = i;
        }
        for (int i = sampleSize; i < populationSize; i++) {
            int j = rand.nextInt(i+1);
            if (j < sampleSize) {
                sampleIndices[j] = i;
            }
        }
    }

    public void process(int value) {
        if (numProcessed < sample.length) {
            sample[numProcessed++] = value;
        } else {
            int j = rand.nextInt(numProcessed+1);
            if (j < sample.length) {
                sample[j] = numProcessed++;
            }
        }
    }

    public String getSamplingMethod() {
        return "reservoir";
    }
    public int[] getSample() { return sample; }
    public int getNumProcessed() { return numProcessed; }
}


package edu.stanford.futuredata.macrobase.analysis.sample;

import java.util.Random;

/**
 * Samples using reservoir sampling
 */
public class ReservoirSampler extends Sampler {
    private Random rand;

    public ReservoirSampler() {
        rand = new Random();
    }

    /**
     * @param populationSize total size of population being sampled from
     * @param samplingRate fraction of population to use as samples
     */
    public void computeSampleIndices(int populationSize, double samplingRate) {
        int sampleSize = (int)(populationSize * samplingRate);
        sampleIndices = new int[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            sampleIndices[i] = i;
        }
        Random rand = new Random();
        for (int i = sampleSize; i < populationSize; i++) {
            int j = rand.nextInt(i+1);
            if (j < sampleSize) {
                sampleIndices[j] = i;
            }
        }
    }

    public String getSamplingMethod() {
        return "reservoir";
    }
}


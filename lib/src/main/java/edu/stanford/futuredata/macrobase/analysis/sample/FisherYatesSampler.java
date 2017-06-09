package edu.stanford.futuredata.macrobase.analysis.sample;

import java.util.Random;

/**
 * Samples using a partial Fisher-Yates shuffle: an array of the population size is shuffled
 * until a sample of the specified size is obtained.
 */
public class FisherYatesSampler extends Sampler {
    private Random rand;

    public FisherYatesSampler() {
        rand = new Random();
    }

    /**
     * @param populationSize total size of population being sampled from
     * @param samplingRate fraction of population to use as samples
     */
    public void computeSampleIndices(int populationSize, double samplingRate) {
        int sampleSize = (int)(populationSize * samplingRate);
        sampleIndices = new int[sampleSize];
        int[] range = new int[populationSize];
        for (int i = 0; i < populationSize; i++) {
            range[i] = i;
        }
        for (int i = 0; i < sampleSize; i++) {
            int j = rand.nextInt(populationSize - i) + i;
            int temp = range[j];
            range[j] = range[i];
            range[i] = temp;
        }
        System.arraycopy(range, 0, sampleIndices, 0, sampleSize);
    }

    public String getSamplingMethod() {
        return "fisher-yates";
    }
}


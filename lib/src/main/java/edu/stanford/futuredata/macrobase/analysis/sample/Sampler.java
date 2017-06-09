package edu.stanford.futuredata.macrobase.analysis.sample;

/**
 * Abstract class for sampling. Should not modify input.
 * computeSampleIndices computes and stores the indices of samples.
 * getSamplingMethod returns the type of the sampler.
 * getSamples returns an array of samples from the input using the computed indices.
 */
public abstract class Sampler {
	protected int[] sampleIndices;

    public abstract void computeSampleIndices(int populationSize, double samplingRate);
    public abstract String getSamplingMethod();

    /**
     * @param input the population from which we take samples, should have size equal to
     * populationSize
     */
    public double[] getSample(double[] input) {
        int sampleSize = sampleIndices.length;
        double[] samples = new double[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            samples[i] = input[sampleIndices[i]];
        }
        return samples;
    }

    public String[] getSample(String[] input) {
        int sampleSize = sampleIndices.length;
        String[] samples = new String[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            samples[i] = input[sampleIndices[i]];
        }
        return samples;
    }
}

package edu.stanford.futuredata.macrobase.analysis.sample;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SamplerTest {
    private int populationSize = 1000;
    private double samplingRate = 0.1;
    private int sampleSize;
    private double[] inputDoubles;
    private String[] inputStrings;

    @Before
    public void setUp() {
        sampleSize = (int)(populationSize * samplingRate);
        
        inputDoubles = new double[populationSize];
        for (int i = 0; i < populationSize; i++) {
            inputDoubles[i] = i;
        }

        inputStrings = new String[populationSize];
        for (int i = 0; i < populationSize; i++) {
            inputStrings[i] = String.valueOf(i);
        }
    }

    @Test
    public void testReservoirSampler() throws Exception {
        Sampler sampler = new ReservoirSampler();
        sampler.computeSampleIndices(populationSize, samplingRate);
        assertEquals(sampleSize, sampler.sampleIndices.length);
        
        double[] sampleDoubles = sampler.getSample(inputDoubles);
        assertEquals(sampleDoubles.length, sampleSize);

        String[] sampleStrings = sampler.getSample(inputStrings);
        assertEquals(sampleDoubles.length, sampleSize);

        assertEquals(sampler.getSamplingMethod(), "reservoir");
    }

    @Test
    public void testFisherYatesSampler() throws Exception {
        Sampler sampler = new FisherYatesSampler();
        sampler.computeSampleIndices(populationSize, samplingRate);
        assertEquals(sampleSize, sampler.sampleIndices.length);
        
        double[] sampleDoubles = sampler.getSample(inputDoubles);
        assertEquals(sampleDoubles.length, sampleSize);

        String[] sampleStrings = sampler.getSample(inputStrings);
        assertEquals(sampleDoubles.length, sampleSize);

        assertEquals(sampler.getSamplingMethod(), "fisher-yates");
    }
}
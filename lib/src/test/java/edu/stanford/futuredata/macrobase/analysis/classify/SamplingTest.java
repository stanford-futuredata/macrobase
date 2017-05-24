package edu.stanford.futuredata.macrobase.analysis.classify;

import org.junit.Test;
import java.util.Random;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SamplingTest {
	private long startTime = 0;
	private long time = 0;

	@Test
    public void testSampling() throws Exception {
    	int[] rowSizes = {100000, 1000000, 10000000};
    	double[] sampleProportions = {0.5, 0.4, 0.3, 0.2, 0.1, 0.05, 0.025, 0.01};

    	List<String> lines = new ArrayList<String>();

    	// Warm up the JVM
        for (int i = 0; i < 25; i++) {
        	reservoir(1000000, 0.25);
        	rejection(1000000, 0.25);
        	fisheryates(1000000, 0.25);
        }

    	for (int rowSize : rowSizes) {
    		for (double sampleProportion : sampleProportions) {
    			startTime = System.currentTimeMillis();
		    	for (int i = 0; i < 100; i++) {
		    		reservoir(rowSize, sampleProportion);
		    	}
		    	time = System.currentTimeMillis() - startTime;
		    	System.out.format("Reservoir, %d, sample %.3f: %d\n", rowSize, sampleProportion, time);
		    	lines.add("reservoir, " + String.valueOf(rowSize) + ", " + String.valueOf(sampleProportion) +
		    		", " + String.valueOf(time));
    		}
    	}

    	for (int rowSize : rowSizes) {
    		for (double sampleProportion : sampleProportions) {
    			startTime = System.currentTimeMillis();
		    	for (int i = 0; i < 100; i++) {
		    		rejection(rowSize, sampleProportion);
		    	}
		    	time = System.currentTimeMillis() - startTime;
		    	System.out.format("Rejection, %d, sample %.3f: %d\n", rowSize, sampleProportion, time);
		    	lines.add("rejection, " + String.valueOf(rowSize) + ", " + String.valueOf(sampleProportion) +
		    		", " + String.valueOf(time));
    		}
    	}

    	for (int rowSize : rowSizes) {
    		for (double sampleProportion : sampleProportions) {
    			startTime = System.currentTimeMillis();
		    	for (int i = 0; i < 100; i++) {
		    		fisheryates(rowSize, sampleProportion);
		    	}
		    	time = System.currentTimeMillis() - startTime;
		    	System.out.format("Fisher-Yates, %d, sample %.3f: %d\n", rowSize, sampleProportion, time);
		    	lines.add("fisheryates, " + String.valueOf(rowSize) + ", " + String.valueOf(sampleProportion) +
		    		", " + String.valueOf(time));
    		}
    	}

    	Path file = Paths.get("sampling.csv");
		Files.write(file, lines, Charset.forName("UTF-8"));
    }

    public int[] reservoir(int numRows, double sampleProportion) {
    	int sampleSize = (int)(numRows * sampleProportion);
        int[] sampleIndices = new int[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            sampleIndices[i] = i;
        }
        Random rand = new Random();
        for (int i = sampleSize; i < numRows; i++) {
            int j = rand.nextInt(i+1);
            if (j < sampleSize) {
                sampleIndices[j] = i;
            }
        }
        return sampleIndices;
    }

    public int[] rejection(int numRows, double sampleProportion) {
    	int sampleSize = (int)(numRows * sampleProportion);
    	boolean[] mask = new boolean[numRows];
        int[] sampleIndices = new int[sampleSize];
        int numSamples = 0;
        Random rand = new Random();
        while (true) {
            int sample = rand.nextInt(numRows);
            if (mask[sample] == false) {
                mask[sample] = true;
                sampleIndices[numSamples] = sample;
                numSamples++;
                if (numSamples == sampleSize) {
                    break;
                }
            }
        }
        return sampleIndices;
    }

    public int[] fisheryates(int numRows, double sampleProportion) {
    	int sampleSize = (int)(numRows * sampleProportion);
    	int[] range = new int[numRows];
        for (int i = 0; i < numRows; i++) {
            range[i] = i;
        }
        Random rand = new Random();
        for (int i = 0; i < sampleSize; i++) {
            int j = rand.nextInt(numRows - i) + i;
            int temp = range[j];
            range[j] = range[i];
            range[i] = temp;
        }
        int[] sampleIndices = new int[sampleSize];
        System.arraycopy(range, 0, sampleIndices, 0, sampleSize);
        return sampleIndices;
    }
}
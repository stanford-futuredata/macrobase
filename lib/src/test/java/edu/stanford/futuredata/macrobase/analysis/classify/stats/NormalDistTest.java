package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import org.junit.Test;

import static org.junit.Assert.*;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class NormalDistTest {
    @Test
    public void testCDF() throws Exception {
        NormalDist dist = new NormalDist();
        double cdf;

        cdf = dist.cdf(0.0, 1.0, 0.5);
        assertEquals(0.6915, cdf, 0.001);

        cdf = dist.cdf(5.0, 2.0, 4.0);
        assertEquals(0.3085, cdf, 0.001);

        // Test interpolation
        cdf = dist.cdf(0.0, 1.0, 0.755);
        assertEquals(0.7749, cdf, 0.001);

        // Test max z-score
        cdf = dist.cdf(0.0, 1.0, 4.0);
        assertEquals(1.0, cdf, 0.001);

        // Test min z-score
        cdf = dist.cdf(0.0, 1.0, -4.0);
        assertEquals(0.0, cdf, 0.001);
    }

    @Test
    public void constructLUT() {
        Map<Integer, Double> LUT = new HashMap<Integer, Double>();
        NormalDistribution dist = new NormalDistribution();
        int minKey = (int) Math.round(NormalDist.MINZSCORE / NormalDist.GRANULARITY);
        int maxKey = (int) Math.round(NormalDist.MAXZSCORE / NormalDist.GRANULARITY);
        for (int i = minKey; i <= maxKey; i ++) {
            double zscore = i * NormalDist.GRANULARITY;
            LUT.put(i, dist.cumulativeProbability(zscore));
        }

        try {
            File file = new File("src/main/resources/cdfLUT.ser");
            FileOutputStream f = new FileOutputStream(file, false);
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(LUT);
            o.close();
            f.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File to save NormalDist LUT not found");
        } catch (IOException e) {
            throw new RuntimeException("Error initializing NormalDist LUT stream");
        }
    }
}
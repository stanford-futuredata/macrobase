package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import java.io.*;
import java.lang.Math;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a look-up table in order to quickly obtain the CDF of a
 * normal distribution.
 */
public class NormalDist {
    public static final double GRANULARITY = 0.01;
    public static final double MAXZSCORE = 3.5;
    public static final double MINZSCORE = -3.5;

    private Map<Integer, Double> cdfLUT;

    public NormalDist() {
        loadLUT();
    }

    public double cdf(double mean, double std, double x) {
        double zscore = (x - mean) / std;

        if (zscore > MAXZSCORE) {
            return 1.0;
        }
        if (zscore < MINZSCORE) {
            return 0.0;
        }

        // Interpolate the CDF
        double exactEntry = zscore / GRANULARITY;
        int lowerEntry = (int) Math.floor(exactEntry);
        int higherEntry = (int) Math.ceil(exactEntry);

        if (lowerEntry == higherEntry) {
            return cdfLUT.get(lowerEntry);
        } else {
            return cdfLUT.get(lowerEntry) +
                    ((exactEntry - lowerEntry) * (cdfLUT.get(higherEntry) - cdfLUT.get(lowerEntry)));
        }
    }

    @SuppressWarnings("unchecked")
    private void loadLUT() {
        try {
            URL url = this.getClass().getResource("/cdfLUT.ser");
            File file = new File(url.getPath());
            FileInputStream f = new FileInputStream(file);
            ObjectInputStream s = new ObjectInputStream(f);
            cdfLUT = (HashMap<Integer, Double>) s.readObject();
            s.close();
            f.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File with NormalDist LUT not found");
        } catch (IOException e) {
            throw new RuntimeException("Error initializing NormalDist LUT stream");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("NormalDist LUT has wrong class");
        }
    }
}

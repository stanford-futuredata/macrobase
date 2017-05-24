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
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

public class TrainingTest {
	private long startTime = 0;
	private long time = 0;
    private double median;
    private double MAD;
    private double[] metrics;
    private double[] metricsCopy;
    private int numIterations = 100;
    private final double trimmedMeanFallback = 0.05;

	@Test
    public void testTraining() throws Exception {
    	int[] rowSizes = {10000, 25000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000};

    	List<String> lines = new ArrayList<String>();
        Random rand = new Random();

        // Warm up the JVM
        metrics = new double[5000000];
        for (int i = 0 ; i < 5000000; i++) {
            metrics[i] = rand.nextDouble();
        }
        for (int i = 0; i < 25; i++) {
            train_old(metrics);
            train_iqr(metrics);
            train(metrics);
            train_iqr_old(metrics);
        }

    	for (int rowSize : rowSizes) {
            // if (rowSize < 2500000) {
            //     numIterations = 1000;
            // } else {
            //     numIterations = 100;
            // }
            numIterations = 100000000 / rowSize;

            metrics = new double[rowSize];
            for (int i = 0 ; i < rowSize; i++) {
                metrics[i] = rand.nextDouble();
            }
            metricsCopy = new double[rowSize];

			startTime = System.currentTimeMillis();
	    	for (int i = 0; i < numIterations; i++) {
                System.arraycopy(metrics, 0, metricsCopy, 0, rowSize);
	    		train_old(metricsCopy);
	    	}
	    	time = System.currentTimeMillis() - startTime;
	    	System.out.format("Train old, %d: %d\n", rowSize, time);
	    	lines.add("train_old, " + String.valueOf(rowSize) +
	    		", " + String.valueOf(time));

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                System.arraycopy(metrics, 0, metricsCopy, 0, rowSize);
                train_iqr(metricsCopy);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Train IQR, %d: %d\n", rowSize, time);
            lines.add("train_iqr, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                System.arraycopy(metrics, 0, metricsCopy, 0, rowSize);
                train(metricsCopy);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Train, %d: %d\n", rowSize, time);
            lines.add("train, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                System.arraycopy(metrics, 0, metricsCopy, 0, rowSize);
                train_iqr_old(metricsCopy);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Train IQR old, %d: %d\n", rowSize, time);
            lines.add("train_iqr_old, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));
    	}

    	Path file = Paths.get("training.csv");
		Files.write(file, lines, Charset.forName("UTF-8"));
    }

    public void train_old(double[] metrics) {
        int len = metrics.length;

        Arrays.sort(metrics);

        if (len % 2 == 0) {
            median = (metrics[len / 2 - 1] + metrics[len / 2]) / 2;
        } else {
            median = metrics[(int) Math.ceil(len / 2)];
        }

        double[] residuals = new double[len];
        for (int i = 0; i < len; i++) {
            residuals[i] = Math.abs(metrics[i] - median);
        }

        Arrays.sort(residuals);

        if (len % 2 == 0) {
            MAD = (residuals[len / 2 - 1] +
                   residuals[len / 2]) / 2;
        } else {
            MAD = residuals[(int) Math.ceil(len / 2)];
        }

        if (MAD == 0) {
            int lowerTrimmedMeanIndex = (int) (residuals.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (residuals.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += residuals[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }
    }

    public void train(double[] metrics) {
        median = new Percentile().evaluate(metrics, 50);

        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = Math.abs(metrics[i] - median);
        }

        MAD = new Percentile().evaluate(metrics, 50);

        if (MAD == 0) {
            Arrays.sort(metrics);
            int lowerTrimmedMeanIndex = (int) (metrics.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (metrics.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += metrics[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }
    }

    public void train_iqr(double[] metrics) {
        Percentile percentile = new Percentile();
        percentile.setData(metrics);
        median = percentile.evaluate(50);
        // MAD is set as half of IQR
        MAD = (percentile.evaluate(75) - percentile.evaluate(25)) / 2;

        if (MAD == 0) {
            for (int i = 0; i < metrics.length; i++) {
                metrics[i] = Math.abs(metrics[i] - median);
            }

            Arrays.sort(metrics);
            int lowerTrimmedMeanIndex = (int) (metrics.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (metrics.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += metrics[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }
    }

    public void train_iqr_old(double[] metrics) {
        Percentile percentile = new Percentile();
        median = percentile.evaluate(metrics, 50);
        // MAD is set as half of IQR
        MAD = (percentile.evaluate(metrics, 75) - percentile.evaluate(metrics, 25)) / 2;

        if (MAD == 0) {
            for (int i = 0; i < metrics.length; i++) {
                metrics[i] = Math.abs(metrics[i] - median);
            }

            Arrays.sort(metrics);
            int lowerTrimmedMeanIndex = (int) (metrics.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (metrics.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += metrics[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }
    }
}
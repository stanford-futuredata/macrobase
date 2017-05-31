package edu.stanford.futuredata.macrobase.analysis.classify;

import org.junit.Test;
import java.lang.Long;
import java.lang.Thread;
import java.util.Random;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.concurrent.*;

public class ScoringTest {
	private long startTime = 0;
	private long time = 0;
    // private double median = 0.5;
    // private double MAD = 0.2;
    // private double cutoff = 2.576;
    private double highCutoff = 0.9;
    private double lowCutoff = 0.1;
    private boolean includeHigh = true;
    private boolean includeLow = true;
    private double[] metrics;
    private int numIterations = 100;

    private static final int procs = Runtime.getRuntime().availableProcessors();
    private ExecutorService executorService = Executors.newFixedThreadPool(procs);

	@Test
    public void testScoring() throws Exception {
    	int[] rowSizes = {10000, 25000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000};

    	List<String> lines = new ArrayList<String>();
        Random rand = new Random();

        // Warm up the JVM
        metrics = new double[5000000];
        for (int i = 0 ; i < 5000000; i++) {
            metrics[i] = rand.nextDouble();
        }
        for (int i = 0; i < 25; i++) {
            parallelStream(metrics);
            executor(metrics);
            parallelLambda(metrics);
            serial(metrics);
            thread(metrics);
        }

    	for (int rowSize : rowSizes) {
            // if (rowSize < 2500000) {
            //     numIterations = 1000;
            // } else {
            //     numIterations = 100;
            // }
            numIterations = 2000000000 / rowSize;

            metrics = new double[rowSize];
            for (int i = 0 ; i < rowSize; i++) {
                metrics[i] = rand.nextDouble();
            }

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                parallelStream(metrics);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Parallel stream, %d: %d\n", rowSize, time);
            lines.add("parallel_stream, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                executor(metrics);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Executor, %d: %d\n", rowSize, time);
            lines.add("executor_service, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                parallelLambda(metrics);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Parallel lambda, %d: %d\n", rowSize, time);
            lines.add("parallel_lambda, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                serial(metrics);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Serial, %d: %d\n", rowSize, time);
            lines.add("serial, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numIterations; i++) {
                thread(metrics);
            }
            time = System.currentTimeMillis() - startTime;
            System.out.format("Threads, %d: %d\n", rowSize, time);
            lines.add("threads, " + String.valueOf(rowSize) +
                ", " + String.valueOf(time));
    	}

    	Path file = Paths.get("scoring.csv");
		Files.write(file, lines, Charset.forName("UTF-8"));
    }

    public void parallelStream(double[] metrics) {
        double[] outliers = IntStream.range(0, metrics.length).parallel().mapToDouble(
            r -> ((metrics[r] > highCutoff && includeHigh) || (metrics[r] < lowCutoff && includeLow)) ? 1.0 : 0.0
        ).toArray();
    }

    public void parallelLambda(double[] metrics) {
        double[] results = new double[metrics.length];
        System.arraycopy(metrics, 0, results, 0, metrics.length);
        Arrays.parallelSetAll(results, m -> ((m > highCutoff && includeHigh) || (m < lowCutoff && includeLow)) ? 1.0 : 0.0);
    }

    public void serial(double[] metrics) {
        double[] results = new double[metrics.length];
        for (int r = 0; r < metrics.length; r++) {
            boolean isOutlier = (metrics[r] > highCutoff && includeHigh) ||
                (metrics[r] < lowCutoff && includeLow);
            results[r] = isOutlier ? 1.0 : 0.0;
        }
    }

    public void executor(double[] metrics) {
        double[] results = new double[metrics.length];
        // System.arraycopy(metrics, 0, results, 0, metrics.length);
        // List<Future<double[]>> futures = new ArrayList<>();
        executorService = Executors.newFixedThreadPool(procs);
        int blockSize = (results.length + procs - 1) / procs;
        for(int j = 0; j < procs; j++) {
            int start = j * blockSize;
            int end = Math.min(results.length, (j + 1) * blockSize);
            executorService.submit(new Scorer(metrics, results, start, end, lowCutoff, highCutoff));
            // futures.add(executorService.submit(new Scorer(Arrays.copyOfRange(metrics, start, end), lowCutoff, highCutoff)));
        }
        executorService.shutdown();
        try {
          executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          System.exit(1);
        }
        // double[] results = new double[metrics.length];
        // int curr = 0;
        // for(Future<double[]> future: futures) {
        //     try {
        //         double[] result = future.get();
        //         for (int j = curr; j < curr + result.length; j++) {
        //             results[j] = result[j - curr];
        //         }
        //         curr += result.length;
        //     } catch(Exception e) {
        //         System.exit(1);
        //     }
        // }
    }

    public void thread(double[] metrics) {
        double[] results = new double[metrics.length];
        int blockSize = (results.length + procs - 1) / procs;
        Thread[] threads = new Thread[procs];
        for (int j = 0; j < procs; j++) {
            int start = j * blockSize;
            int end = Math.min(results.length, (j + 1) * blockSize);
            threads[j] = new Thread(new Scorer(metrics, results, start, end, lowCutoff, highCutoff));
            threads[j].start();
        }
        for (int j = 0; j < procs; j++) {
            try {
                threads[j].join();
            } catch (InterruptedException e) {
                System.exit(1);
            }
        }
    }
}
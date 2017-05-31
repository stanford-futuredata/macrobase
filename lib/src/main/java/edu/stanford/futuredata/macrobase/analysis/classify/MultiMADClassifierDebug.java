package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;

import edu.stanford.futuredata.macrobase.analysis.stats.MAD;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.lang.Math;
import java.lang.Double;

import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.IntStream;

public class MultiMADClassifierDebug implements Transformer {
    private double percentile = 0.5;
    private double cutoff = 2.576;
    private int samplingRate = 1;
    private List<String> columnNames;
    private String attributeName;
    private String outputColumnName = "_OUTLIER";
    private List<Double> medians;
    private List<Double> MADs;
    public List<Double> upperBoundsMedian;
    public List<Double> lowerBoundsMedian;
    public List<Double> upperBoundsMAD;
    public List<Double> lowerBoundsMAD;
    private long startTime = 0;
    private long trainTime = 0;
    private long scoreTime = 0;
    private long otherTime = 0;
    private long samplingTime = 0;
    private static final int procs = Runtime.getRuntime().availableProcessors();
    private final ExecutorService executorService = Executors.newFixedThreadPool(procs);

    // Calculated values
    private DataFrame output;

    public MultiMADClassifierDebug(String attributeName, String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        this.attributeName = attributeName;
        this.medians = new ArrayList<Double>();
        this.MADs = new ArrayList<Double>();
        this.upperBoundsMedian = new ArrayList<Double>();
        this.lowerBoundsMedian = new ArrayList<Double>();
        this.upperBoundsMAD = new ArrayList<Double>();
        this.lowerBoundsMAD = new ArrayList<Double>();
    }

    @Override
    public void process(DataFrame input) {

        output = input.copy();
        double[] resultColumn = new double[input.getNumRows()];

        // long startTime = System.currentTimeMillis();

        int[] sampleIndices = new int[input.numRows / samplingRate];
        if (samplingRate != 1) {
            sampleIndices = getRandomSample(input);
        }

        // samplingTime += (System.currentTimeMillis() - startTime);

        // double[][] metrics = new double[columnNames.size()][input.getNumRows()];

        MAD mad = new MAD();
        double[] attributes = input.getDoubleColumnByName(attributeName);

        // for (String column : columnNames) {
        for (int i = 0; i < columnNames.size(); i++) {
            startTime = System.currentTimeMillis();

            double[] metrics = input.getDoubleColumnByName(columnNames.get(i));
            // metrics[i] = input.getDoubleColumnByName(columnNames.get(i));
            // double[] trainingMetrics = trainingInput.getDoubleColumnByName(columnNames.get(i));

            double[] trainingMetrics = new double[input.numRows / samplingRate];
            if (samplingRate == 1) {
                // trainingMetrics = metrics;
                System.arraycopy(metrics, 0, trainingMetrics, 0, metrics.length);
            } else {
                // int[] sampleIndices = getRandomSample(input);
                for (int j = 0; j < sampleIndices.length; j++) {
                    trainingMetrics[j] = metrics[sampleIndices[j]];
                }
            }

            // // For bootstrapping
            // double[] bootstrapMetrics = new double[trainingMetrics.length];
            // System.arraycopy(trainingMetrics, 0, bootstrapMetrics, 0, trainingMetrics.length);
            
            trainTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            mad.train(trainingMetrics);
            medians.add(mad.getMedian());
            MADs.add(mad.getMAD());
            double lowCutoff = mad.getMedian() - (cutoff * mad.getMAD());
            double highCutoff = mad.getMedian() + (cutoff * mad.getMAD());

            trainTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            // // Bootstrap the confidence interval
            // bootstrap(bootstrapMetrics, mad.getMedian(), mad.getMAD());

            otherTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            // // Parallelized stream scoring:
            // double adjustedMAD = mad.MAD * mad.MAD_TO_ZSCORE_COEFFICIENT;
            // double[] outliers = IntStream.range(0, input.getNumRows()).parallel().mapToDouble(r -> Math.abs(metrics[r] - mad.median) / adjustedMAD > cutoff ? 1.0 : 0.0).toArray();
            // // System.out.format("Column %d has %d outliers\n", i, outliers.length);
            // output.addDoubleColumn(columnNames.get(i) + outputColumnName, outliers);


            // // Multi-threaded scoring
            // double[] results = new double[metrics.length];
            // System.arraycopy(metrics, 0, results, 0, metrics.length);
            // int blockSize = (results.length + procs - 1) / procs;
            // Thread[] threads = new Thread[procs];
            // for (int j = 0; j < procs; j++) {
            //     int start = j * blockSize;
            //     int end = Math.min(results.length, (j + 1) * blockSize);
            //     threads[j] = new Thread(new Scorer(results, start, end, lowCutoff, highCutoff));
            //     threads[j].start();
            // }
            // for (int j = 0; j < procs; j++) {
            //     try {
            //         threads[j].join();
            //     } catch (InterruptedException e) {
            //         System.exit(1);
            //     }
            // }

            // // Parallel lambdas scoring:
            // double adjustedMAD = mad.MAD * mad.MAD_TO_ZSCORE_COEFFICIENT;
            // double[] results = new double[metrics.length];
            // System.arraycopy(metrics, 0, results, 0, metrics.length);
            // Arrays.parallelSetAll(results, m -> Math.abs(m - mad.median) / adjustedMAD > cutoff ? 1.0 : 0.0);
            // output.addDoubleColumn(columnNames.get(i) + outputColumnName, results);

            // Original Scoring:
            int numInliers = 0;
            int numOutliers = 0;
            HashMap<Double, List<MutableInt>> counts = new HashMap<Double, List<MutableInt>>();
            double[] results = new double[metrics.length];
            for (int r = 0; r < input.getNumRows(); r++) {
                boolean isOutlier = (metrics[r] > highCutoff) || (metrics[r] < lowCutoff);
                results[r] = isOutlier ? 1.0 : 0.0;
                if (isOutlier) {
                    numOutliers++;
                } else {
                    numInliers++;
                }
                if (counts.containsKey(attributes[r])) {
                    counts.get(attributes[r]).get(isOutlier ? 0 : 1).increment();
                } else {
                    counts.put(attributes[r], isOutlier ?
                        Arrays.asList(new MutableInt(1), new MutableInt(0)) :
                        Arrays.asList(new MutableInt(0), new MutableInt(1)));
                }
            }
            System.out.format("Inliers: %d, Outliers: %d\n", numInliers, numOutliers);
            for (Double attr : counts.keySet()) {
                List<MutableInt> numInOut = counts.get(attr);
                System.out.format("Attribute: %.0f, inliers: %d, outliers: %d\n", attr,
                    numInOut.get(1).get(), numInOut.get(0).get());
            }
            output.addDoubleColumn(columnNames.get(i) + outputColumnName, results);

            // // Executer service scoring:
            // double adjustedMAD = mad.MAD * mad.MAD_TO_ZSCORE_COEFFICIENT;
            // List<Future<double[]>> futures = new ArrayList<>();
            // int blockSize = (metrics.length + procs - 1) / procs;
            // for(int j = 0; j < procs; j++) {
            //     int start = j * blockSize;
            //     int end = Math.min(metrics.length, (j + 1) * blockSize);
            //     futures.add(executorService.submit(new Scorer(Arrays.copyOfRange(metrics, start, end), mad.median, adjustedMAD, cutoff)));
            // }
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
            // output.addDoubleColumn(columnNames.get(i) + outputColumnName, results);

            // // Old Original Scoring:
            // for (int r = 0; r < input.getNumRows(); r++) {
            //     double curVal = metrics[r];
            //     double score = mad.zscore(curVal);
            //     if (score >= cutoff) {
            //         resultColumn[r] = 1.0;
            //     }
            // }

            // // Old Parallelized stream scoring:
            // int[] outliers = IntStream.range(0, input.getNumRows()).parallel().filter(r -> mad.zscore(metrics[r]) >= cutoff).toArray();
            // // System.out.format("Column %d has %d outliers\n", i, outliers.length);
            // for (int r : outliers) {
            //     resultColumn[r] = 1.0;
            // }

            scoreTime += (System.currentTimeMillis() - startTime);
        }

        startTime = System.currentTimeMillis();
        
        // Even more parallelized scoring
        // RealMatrix metrics_matrix = new Array2DRowRealMatrix(metrics);
        // resultColumn = IntStream.range(0, input.getNumRows()).parallel().map(
        //     i -> (Arrays.stream(metrics_matrix.getColumn(i)).map(m -> mad.score(m)).max().getAsDouble() >= cutoff) ? 1 : 0
        // ).asDoubleStream().toArray();

        // RealMatrix metrics_matrix = new Array2DRowRealMatrix(metrics);
        // resultColumn = IntStream.range(0, input.getNumRows()).parallel().map(
        //     i -> (Arrays.stream(metrics_matrix.getColumn(i)).map(m -> mad.zscore(m)).filter(s -> s > cutoff).count() > 0) ? 1 : 0
        // ).asDoubleStream().toArray();

        scoreTime += (System.currentTimeMillis() - startTime);

        // input.addDoubleColumn(outputColumnName, resultColumn);
        // output.addDoubleColumn(outputColumnName, resultColumn);
    }

    private int[] getRandomSample(DataFrame input) {
        long startSamplingTime = System.currentTimeMillis();

        // // Inefficient shuffling implementation:
        // // Integer[] arr = new Integer[input.getNumRows()];
        // List<Integer> arr = new ArrayList<Integer>();
        // for (int i = 0; i < input.getNumRows(); i++) {
        //     // arr[i] = i;
        //     arr.add(i);
        // }
        // // Collections.shuffle(Arrays.asList(arr));
        // Collections.shuffle(arr);

        // int sampleSize = input.getNumRows() / samplingRate;
        // int[] sampleIndices = new int[sampleSize];
        // for (int i = 0; i < sampleSize; i++) {
        //     // sampleIndices[i] = arr[i];
        //     sampleIndices[i] = arr.get(i);
        // }

        // // Reservoir Sampling:
        // int sampleSize = input.numRows / samplingRate;
        // int[] sampleIndices = new int[sampleSize];
        // for (int i = 0; i < sampleSize; i++) {
        //     sampleIndices[i] = i;
        // }
        // Random rand = new Random();
        // for (int i = sampleSize; i < input.numRows; i++) {
        //     int j = rand.nextInt(i+1);
        //     if (j < sampleSize) {
        //         sampleIndices[j] = i;
        //     }
        // }

        // Rejection sampling:
        boolean[] mask = new boolean[input.numRows];
        int sampleSize = input.numRows / samplingRate;
        int[] sampleIndices = new int[sampleSize];
        int numSamples = 0;
        Random rand = new Random();
        while (true) {
            int sample = rand.nextInt(input.numRows);
            if (mask[sample] == false) {
                mask[sample] = true;
                sampleIndices[numSamples] = sample;
                numSamples++;
                if (numSamples == sampleSize) {
                    break;
                }
            }
        }
        
        // // Fisher-Yates
        // int[] range = new int[input.numRows];
        // for (int i = 0; i < input.numRows; i++) {
        //     range[i] = i;
        // }
        // int sampleSize = input.numRows / samplingRate;
        // Random rand = new Random();
        // for (int i = 0; i < sampleSize; i++) {
        //     int j = rand.nextInt(input.numRows - i) + i;
        //     int temp = range[j];
        //     range[j] = range[i];
        //     range[i] = temp;
        // }
        // int[] sampleIndices = new int[sampleSize];
        // System.arraycopy(range, 0, sampleIndices, 0, sampleSize);

        samplingTime += (System.currentTimeMillis() - startSamplingTime);

        return sampleIndices;
        // return input.filter(mask);
    }

    private void bootstrap(double[] trainingMetrics, double median, double MAD) {
        int num_trials = 50;
        int len = trainingMetrics.length;
        double[] bootstrapped_diffs_median = new double[num_trials];
        double[] bootstrapped_diffs_MAD = new double[num_trials];
        Random rand = new Random();
        for (int i = 0; i < num_trials; i++) {
            double[] sample = new double[trainingMetrics.length];
            for (int j = 0; j < trainingMetrics.length; j++) {
                sample[j] = trainingMetrics[rand.nextInt(trainingMetrics.length)];
                // sample[j] = trainingMetrics[j];
            }
            double bootstrap_median = new Percentile().evaluate(sample, 50);
            // System.out.println(bootstrap_median);
            for (int j = 0; j < sample.length; j++) {
                sample[j] = Math.abs(sample[j] - bootstrap_median);
            }
            double bootstrap_MAD = new Percentile().evaluate(sample, 50);

            bootstrapped_diffs_median[i] = bootstrap_median - median;
            bootstrapped_diffs_MAD[i] = bootstrap_MAD - MAD;
        }
        Percentile percentile = new Percentile();
        // System.out.format("%f %f %f\n", median, bootstrapped_diffs_median[18], bootstrapped_diffs_median[18]);
        // System.out.format("[%f %f]\n", median-bootstrapped_diffs[8], median-bootstrapped_diffs[0]);
        upperBoundsMedian.add(median - percentile.evaluate(bootstrapped_diffs_median, 5));
        lowerBoundsMedian.add(median - percentile.evaluate(bootstrapped_diffs_median, 95));
        upperBoundsMAD.add(MAD - percentile.evaluate(bootstrapped_diffs_MAD, 5));
        lowerBoundsMAD.add(MAD - percentile.evaluate(bootstrapped_diffs_MAD, 95));
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    // Parameter Getters and Setters
    public double getPercentile() {
        return percentile;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    /**
     * @param percentile Cutoff point for high or low values
     * @return this
     */
    public MultiMADClassifierDebug setPercentile(double percentile) {
        this.percentile = percentile;
        this.cutoff = Math.sqrt(2) * Erf.erfcInv(2.0*percentile/100.0);
        return this;
    }

    public MultiMADClassifierDebug setCutoff(double cutoff) {
        this.cutoff = cutoff;
        return this;
    }

    /**
     * @param samplingRate Rate to sample elements to calculate MAD
     * @return this
     */
    public MultiMADClassifierDebug setSamplingRate(int samplingRate) {
        this.samplingRate = samplingRate;
        return this;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }

    public List<Double> getMedians() {
        return medians;
    }

    public List<Double> getMADs() {
        return MADs;
    }

    public long getTrainTime() {
        return trainTime;
    }

    public long getScoreTime() {
        return scoreTime;
    }

    public long getOtherTime() {
        return otherTime;
    }

    public long getSamplingTime() {
        return samplingTime;
    }

    public List<Integer> getOutlierIndices() {
        double[] outliers = output.getDoubleColumnByName(outputColumnName);
        List<Integer> outlierIndices = new ArrayList<Integer>();
        for (int i = 0; i < outliers.length; i++) {
            if (outliers[i] == 1.0) {
                outlierIndices.add(i);
            }
        }
        return outlierIndices;
    }

    private class MutableInt {
        private int value;
        public MutableInt(int value) {
            this.value = value;
        }
        public void increment() { ++value;      }
        public int  get()       { return value; }
    }
}

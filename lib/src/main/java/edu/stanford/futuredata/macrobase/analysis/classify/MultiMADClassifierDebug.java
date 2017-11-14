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
    // Bounds based on 95% confidence interval
    public List<Double> upperBoundsMedian;
    public List<Double> lowerBoundsMedian;
    public List<Double> upperBoundsMAD;
    public List<Double> lowerBoundsMAD;
    private double[] bootstrapMetrics;
    private long startTime = 0;
    private long trainTime = 0;
    private long scoreTime = 0;
    private long otherTime = 0;
    private long samplingTime = 0;
    private static final int procs = Runtime.getRuntime().availableProcessors();
    private final ExecutorService executorService = Executors.newFixedThreadPool(procs);
    private final double trimmedMeanFallback = 0.05;
    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;

    enum SamplingMethod {RESERVOIR, FISHER_YATES, REJECTION};
    enum ScoringMethod {PARALLEL_STREAM, THREADS, PARALLEL_LAMBDA, SERIAL, EXECUTOR_SERVICE};
    enum TrainingMethod {MAD_SORT, MAD_PERCENTILE};

    // Change these to configure the classifier
    public boolean includeLow = true;
    public boolean includeHigh = true;
    public SamplingMethod samplingMethod = SamplingMethod.RESERVOIR;
    public ScoringMethod scoringMethod = ScoringMethod.SERIAL;//ScoringMethod.PARALLEL_LAMBDA;
    public TrainingMethod trainingMethod = TrainingMethod.MAD_PERCENTILE;
    public boolean doBootstrap = true;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
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

        startTime = System.currentTimeMillis();

        int[] sampleIndices = new int[input.numRows / samplingRate];
        if (samplingRate != 1) {
            sampleIndices = getRandomSample(input);
        }

        samplingTime += (System.currentTimeMillis() - startTime);

        // double[][] metrics = new double[columnNames.size()][input.getNumRows()];

        MAD mad = new MAD();
        double[] attributes = input.getDoubleColumnByName(attributeName);

        // for (String column : columnNames) {
        for (int i = 0; i < columnNames.size(); i++) {
            double[] metrics = input.getDoubleColumnByName(columnNames.get(i));
            // metrics[i] = input.getDoubleColumnByName(columnNames.get(i));
            // double[] trainingMetrics = trainingInput.getDoubleColumnByName(columnNames.get(i));

            startTime = System.currentTimeMillis();
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
            samplingTime += (System.currentTimeMillis() - startTime);

            /*
            // For bootstrapping
            if (doBootstrap) {
                bootstrapMetrics = new double[trainingMetrics.length];
                System.arraycopy(trainingMetrics, 0, bootstrapMetrics, 0, trainingMetrics.length);
            }
            */

            startTime = System.currentTimeMillis();

//            mad.train(trainingMetrics);
//            medians.add(mad.getMedian());
//            MADs.add(mad.getMAD());
//            lowCutoff = mad.getMedian() - (cutoff * mad.getMAD());
//            highCutoff = mad.getMedian() + (cutoff * mad.getMAD());

            Stats stats = train(trainingMetrics);
            medians.add(stats.median);
            MADs.add(stats.mad);
            lowCutoff = stats.median - (cutoff * stats.mad);
            highCutoff = stats.median + (cutoff * stats.mad);

            trainTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            /*
            // Bootstrap the confidence interval
            if (doBootstrap) {
                bootstrap(bootstrapMetrics, mad.getMedian(), mad.getMAD());
            }
            */

            otherTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            double[] results;
            int blockSize;
            switch(scoringMethod) {
                case PARALLEL_STREAM:
                    // Parallelized stream scoring:
                    double[] outliers = IntStream.range(0, input.getNumRows()).parallel().mapToDouble(
                            r -> ((metrics[r] > highCutoff && includeHigh) || (metrics[r] < lowCutoff && includeLow)) ? 1.0 : 0.0
                    ).toArray();
                    output.addDoubleColumn(columnNames.get(i) + outputColumnName, outliers);
                    break;
                case THREADS:
                    // Multi-threaded scoring
                    results = new double[metrics.length];
                    blockSize = (results.length + procs - 1) / procs;
                    Thread[] threads = new Thread[procs];
                    for (int j = 0; j < procs; j++) {
                        int start = j * blockSize;
                        int end = Math.min(results.length, (j + 1) * blockSize);
                        threads[j] = new Thread(new Scorer(metrics, results, start, end, lowCutoff, highCutoff, includeLow, includeHigh));
                        threads[j].start();
                    }
                    for (int j = 0; j < procs; j++) {
                        try {
                            threads[j].join();
                        } catch (InterruptedException e) {
                            System.exit(1);
                        }
                    }
                    output.addDoubleColumn(columnNames.get(i) + outputColumnName, results);
                    break;
                case PARALLEL_LAMBDA:
                    // Parallel lambdas scoring:
                    results = new double[metrics.length];
                    System.arraycopy(metrics, 0, results, 0, metrics.length);
                    Arrays.parallelSetAll(results, m -> ((m > highCutoff && includeHigh) || (m < lowCutoff && includeLow)) ? 1.0 : 0.0);
                    output.addDoubleColumn(columnNames.get(i) + outputColumnName, results);
                    break;
                case SERIAL:
                    // Original Scoring:
                    results = new double[metrics.length];
                    for (int r = 0; r < input.getNumRows(); r++) {
                        boolean isOutlier = (metrics[r] > highCutoff && includeHigh) ||
                                (metrics[r] < lowCutoff && includeLow);
                        results[r] = isOutlier ? 1.0 : 0.0;
                    }
                    output.addDoubleColumn(columnNames.get(i) + outputColumnName, results);
                    break;
                case EXECUTOR_SERVICE:
                    // Executer service scoring:
                    results = new double[metrics.length];
                    blockSize = (results.length + procs - 1) / procs;
                    for(int j = 0; j < procs; j++) {
                        int start = j * blockSize;
                        int end = Math.min(results.length, (j + 1) * blockSize);
                        executorService.submit(new Scorer(metrics, results, start, end, lowCutoff, highCutoff, includeLow, includeHigh));
                    }
                    executorService.shutdown();
                    try {
                        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        System.exit(1);
                    }
                    output.addDoubleColumn(columnNames.get(i) + outputColumnName, results);
                    break;
            }

            scoreTime += (System.currentTimeMillis() - startTime);
        }

//        // Even more parallelized scoring
//        startTime = System.currentTimeMillis();
//
//         RealMatrix metrics_matrix = new Array2DRowRealMatrix(metrics);
//         resultColumn = IntStream.range(0, input.getNumRows()).parallel().map(
//             i -> (Arrays.stream(metrics_matrix.getColumn(i)).map(m -> mad.score(m)).max().getAsDouble() >= cutoff) ? 1 : 0
//         ).asDoubleStream().toArray();
//
//         RealMatrix metrics_matrix = new Array2DRowRealMatrix(metrics);
//         resultColumn = IntStream.range(0, input.getNumRows()).parallel().map(
//             i -> (Arrays.stream(metrics_matrix.getColumn(i)).map(m -> mad.zscore(m)).filter(s -> s > cutoff).count() > 0) ? 1 : 0
//         ).asDoubleStream().toArray();
//
//        scoreTime += (System.currentTimeMillis() - startTime);
//
//         input.addDoubleColumn(outputColumnName, resultColumn);
//         output.addDoubleColumn(outputColumnName, resultColumn);
    }

    private int[] getRandomSample(DataFrame input) {
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

        int sampleSize = input.numRows / samplingRate;
        int[] sampleIndices = new int[sampleSize];
        Random rand = new Random();
        switch(samplingMethod) {
            case RESERVOIR:
                 // Reservoir Sampling:
                 for (int i = 0; i < sampleSize; i++) {
                     sampleIndices[i] = i;
                 }
                 for (int i = sampleSize; i < input.numRows; i++) {
                     int j = rand.nextInt(i+1);
                     if (j < sampleSize) {
                         sampleIndices[j] = i;
                     }
                 }
                 break;
            case REJECTION:
                // Rejection sampling:
                boolean[] mask = new boolean[input.numRows];
                int numSamples = 0;
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
                break;
            case FISHER_YATES:
                // Fisher-Yates
                int[] range = new int[input.numRows];
                for (int i = 0; i < input.numRows; i++) {
                    range[i] = i;
                }
                for (int i = 0; i < sampleSize; i++) {
                    int j = rand.nextInt(input.numRows - i) + i;
                    int temp = range[j];
                    range[j] = range[i];
                    range[i] = temp;
                }
                System.arraycopy(range, 0, sampleIndices, 0, sampleSize);
                break;
        }

        return sampleIndices;
    }

    private Stats train(double[] metrics) {
        double median = 0;
        double MAD = 1;
        switch(trainingMethod) {
            case MAD_PERCENTILE:
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
                break;
            case MAD_SORT:
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
                break;
        }
        Stats stats = new Stats(median, MAD * MAD_TO_ZSCORE_COEFFICIENT);
        return stats;
    }

    private void bootstrap(double[] trainingMetrics, double median, double MAD) {
        int num_trials = 200;
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
        upperBoundsMedian.add(median - percentile.evaluate(bootstrapped_diffs_median, 2.5));
        lowerBoundsMedian.add(median - percentile.evaluate(bootstrapped_diffs_median, 97.5));
        upperBoundsMAD.add(MAD - percentile.evaluate(bootstrapped_diffs_MAD, 2.5));
        lowerBoundsMAD.add(MAD - percentile.evaluate(bootstrapped_diffs_MAD, 97.5));
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

    private final class Stats {
        private final double median;
        private final double mad;

        public Stats(double median, double mad) {
            this.median = median;
            this.mad = mad;
        }

        public double getMedian() {
            return median;
        }

        public double getMAD() {
            return mad;
        }
    }
}

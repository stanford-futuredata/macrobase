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

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.stream.IntStream;

public class MultiMADClassifier implements Transformer {
    private double percentile = 0.5;
    private double cutoff = 2.576;
    private int samplingRate = 1;
    private List<String> columnNames;
    private String outputColumnName = "_OUTLIER";
    private List<Double> medians;
    private List<Double> MADs;
    private List<Double> upperBounds;
    private List<Double> lowerBounds;
    private long trainTime = 0;
    private long scoreTime = 0;
    private long otherTime = 0;
    private long samplingTime = 0;

    // Calculated values
    private DataFrame output;

    public MultiMADClassifier(String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        this.medians = new ArrayList<Double>();
        this.MADs = new ArrayList<Double>();
        this.upperBounds = new ArrayList<Double>();
        this.lowerBounds = new ArrayList<Double>();
    }

    @Override
    public void process(DataFrame input) {
        output = input.copy();
        double[] resultColumn = new double[input.getNumRows()];

        long startTime = System.currentTimeMillis();

        DataFrame trainingInput = input;
        if (samplingRate != 1) {
            trainingInput = getRandomSample(input);
        }

        samplingTime += (System.currentTimeMillis() - startTime);

        // double[][] metrics = new double[columnNames.size()][input.getNumRows()];

        MAD mad = new MAD();
        // for (String column : columnNames) {
        for (int i = 0; i < columnNames.size(); i++) {
            startTime = System.currentTimeMillis();

            double[] metrics = input.getDoubleColumnByName(columnNames.get(i));
            // metrics[i] = input.getDoubleColumnByName(columnNames.get(i));
            double[] trainingMetrics = trainingInput.getDoubleColumnByName(columnNames.get(i));
            
            otherTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            mad.train(trainingMetrics);
            medians.add(mad.getMedian());
            MADs.add(mad.getMAD());

            trainTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            // Bootstrap the confidence interval
            // bootstrap(trainingMetrics, mad.getMedian());

            otherTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            // Parallelized stream scoring:
            int[] outliers = IntStream.range(0, input.getNumRows()).parallel().filter(r -> mad.zscore(metrics[r]) >= cutoff).toArray();
            for (int r : outliers) {
                resultColumn[r] = 1.0;
            }

            // Original Scoring:
            // for (int r = 0; r < input.getNumRows(); r++) {
            //     double curVal = metrics[r];
            //     double score = mad.zscore(curVal);
            //     if (score >= cutoff) {
            //         resultColumn[r] = 1.0;
            //     }
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

        output.addDoubleColumn(outputColumnName, resultColumn);
    }

    private DataFrame getRandomSample(DataFrame input) {
        // Inefficient shuffling implementation:
        // Integer[] arr = new Integer[input.getNumRows()];
        // for (int i = 0; i < input.getNumRows(); i++) {
        //     arr[i] = i;
        // }
        // Collections.shuffle(Arrays.asList(arr));

        // int sampleSize = input.getNumRows() / samplingRate;
        // boolean[] mask = new boolean[input.getNumRows()];
        // for (int i = 0; i < sampleSize; i++) {
        //     mask[arr[i]] = true;
        // }

        // return input.filter(mask);

        // Reservoir Sampling:
        // int sampleSize = input.getNumRows() / samplingRate;
        // int[] sample_indices = new int[sampleSize];
        // for (int i = 0; i < sampleSize; i++) {
        //     sample_indices[i] = i;
        // }
        // Random rand = new Random();
        // for (int i = sampleSize; i < input.getNumRows(); i++) {
        //     int j = rand.nextInt(i+1);
        //     if (j < sampleSize) {
        //         sample_indices[j] = i;
        //     }
        // }
        // boolean[] mask = new boolean[input.getNumRows()];
        // for (int i = 0; i < sampleSize; i++) {
        //     mask[sample_indices[i]] = true;
        // }
        // return input.filter(mask);

        // Rejection sampling:
        boolean[] mask = new boolean[input.getNumRows()];
        int sampleSize = input.getNumRows() / samplingRate;
        int numSamples = 0;
        Random rand = new Random();
        while (true) {
            int sample = rand.nextInt(input.getNumRows());
            if (mask[sample] == false) {
                mask[sample] = true;
                numSamples++;
                if (numSamples == sampleSize) {
                    break;
                }
            }
        }
        return input.filter(mask);
    }

    private void bootstrap(double[] trainingMetrics, double median) {
        int len = trainingMetrics.length;
        double[] bootstrapped_diffs = new double[10];
        Random rand = new Random();
        for (int i = 0; i < 10; i++) {
            double[] sample = new double[trainingMetrics.length];
            for (int j = 0; j < trainingMetrics.length; j++) {
                sample[j] = trainingMetrics[rand.nextInt(trainingMetrics.length)];
            }
            double bootstrap_median = new Percentile().evaluate(sample, 50);
            bootstrapped_diffs[i] = bootstrap_median - median;
        }
        Percentile percentile = new Percentile();
        // System.out.format("%f %f %f\n", median, bootstrapped_diffs[0], bootstrapped_diffs[8]);
        // System.out.format("[%f %f]\n", median-bootstrapped_diffs[8], median-bootstrapped_diffs[0]);
        upperBounds.add(median - percentile.evaluate(bootstrapped_diffs, 10));
        lowerBounds.add(median - percentile.evaluate(bootstrapped_diffs, 90));
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
    public MultiMADClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        this.cutoff = Math.sqrt(2) * Erf.erfcInv(2.0*percentile/100.0);
        return this;
    }

    /**
     * @param samplingRate Rate to sample elements to calculate MAD
     * @return this
     */
    public MultiMADClassifier setSamplingRate(int samplingRate) {
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

    public List<Double> getUpperBounds() {
        return upperBounds;
    }

    public List<Double> getLowerBounds() {
        return lowerBounds;
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
}

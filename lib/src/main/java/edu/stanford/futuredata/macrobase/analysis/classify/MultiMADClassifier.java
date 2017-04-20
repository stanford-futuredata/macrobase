package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;

import edu.stanford.futuredata.macrobase.analysis.stats.MAD;

import org.apache.commons.math3.special.Erf;

import java.lang.Math;
import java.lang.Double;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

public class MultiMADClassifier implements Transformer {
    private double percentile = 0.5;
    private double cutoff = 2.576;
    private int samplingRate = 1;
    private List<String> columnNames;
    private String outputColumnName = "_OUTLIER";
    private List<Double> medians;
    private List<Double> MADs;

    // Calculated values
    private DataFrame output;

    public MultiMADClassifier(String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        this.medians = new ArrayList<Double>();
        this.MADs = new ArrayList<Double>();
    }

    @Override
    public void process(DataFrame input) {
        long trainTime = 0;
        long scoreTime = 0;
        long otherTime = 0;

        output = input.copy();
        double[] resultColumn = new double[input.getNumRows()];

        for (String column : columnNames) {
            long startTime = System.currentTimeMillis();

            double[] metrics = input.getDoubleColumnByName(column);
            double[] trainingMetrics = Arrays.copyOf(metrics, metrics.length);
            if (samplingRate != 1) {
                trainingMetrics = IntStream.range(0, metrics.length)
                    .filter(i -> i % samplingRate == 0)
                    .mapToDouble(i -> metrics[i])
                    .toArray();
            }
            
            otherTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            MAD mad = new MAD();
            mad.train(trainingMetrics);
            medians.add(mad.getMedian());
            MADs.add(mad.getMAD());

            trainTime += (System.currentTimeMillis() - startTime);
            startTime = System.currentTimeMillis();

            for (int i = 0; i < input.getNumRows(); i++) {
                double curVal = metrics[i];
                double score = mad.getZScoreEquivalent(mad.score(curVal));
                if (score >= cutoff) {
                    resultColumn[i] = 1.0;
                }
            }

            scoreTime += (System.currentTimeMillis() - startTime);
        }
        output.addDoubleColumn(outputColumnName, resultColumn);

        System.out.format("train: %d ms, score: %d ms, other: %d ms\n", trainTime, scoreTime, otherTime);
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
}

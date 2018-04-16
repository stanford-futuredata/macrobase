package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.sample.ReservoirSampler;
import edu.stanford.futuredata.macrobase.analysis.sample.Sampler;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Classify rows based on high / low values for a single column. Returns a new DataFrame with a
 * column representation the classification status for each row: 1.0 if outlier, 0.0 otherwise.
 */
public class PercentileClassifier extends Classifier implements ThresholdClassifier {
    private Logger log = LoggerFactory.getLogger("PercentileClassifier");

    // Parameters
    private double percentile = 0.5;
    private boolean includeHigh = true;
    private boolean includeLow = true;
    private double sampleRate = 1.0;
    private int outlierSampleSize = -1;
    private int inlierSampleSize = -1;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;

    public PercentileClassifier(String columnName) {
        super(columnName);
    }

    @Override
    public void process(DataFrame input) {
        long start, elapsed;

        double[] metrics = input.getDoubleColumnByName(columnName);
        int len = metrics.length;
        start = System.nanoTime();
        lowCutoff = new Percentile().evaluate(metrics, percentile);
        highCutoff = new Percentile().evaluate(metrics, 100.0 - percentile);
        elapsed = System.nanoTime() - start;
        log.info("Cutoff eval time: {} ms", elapsed / 1.e6);

        start = System.nanoTime();
        if (outlierSampleSize > 0 && inlierSampleSize > 0 && outlierSampleSize + inlierSampleSize < len) {
            ReservoirSampler outlierSampler = new ReservoirSampler(outlierSampleSize);
            ReservoirSampler inlierSampler = new ReservoirSampler(inlierSampleSize);

            for (int i = 0; i < len; i++) {
                double curVal = metrics[i];
                if ((curVal > highCutoff && includeHigh)
                        || (curVal < lowCutoff && includeLow)
                        ) {
                    outlierSampler.process(i);
                } else {
                    inlierSampler.process(i);
                }
            }
            numOutliers = outlierSampler.getNumProcessed();

            int[] sampledOutlierIndices = outlierSampler.getSample();
            int[] sampledInlierIndices = inlierSampler.getSample();
            int actualOutlierSampleSize = Math.min(outlierSampleSize, numOutliers);
            int actualInlierSampleSize = Math.min(inlierSampleSize, inlierSampler.getNumProcessed());

            int sampleSize = actualOutlierSampleSize + actualInlierSampleSize;
            int[] sampleIndices = new int[sampleSize];

            int idx = 0;
            for (int i = 0; i < actualOutlierSampleSize; i++) {
                sampleIndices[idx++] = sampledOutlierIndices[i];
            }
            for (int i = 0; i < actualInlierSampleSize; i++) {
                sampleIndices[idx++] = sampledInlierIndices[i];
            }

            output = input.sample(sampleIndices);
            double[] resultColumn = new double[sampleSize];
            for (int i = 0; i < actualOutlierSampleSize; i++) {
                resultColumn[i] = 1.0;
            }
            output.addColumn(outputColumnName, resultColumn);
            inlierWeight = ((double) (len - numOutliers) / actualInlierSampleSize) / ((double) numOutliers / actualOutlierSampleSize);
            outlierSampleRate = (double) actualOutlierSampleSize / numOutliers;
        } else if (sampleRate < 1.0) {
            int sampleSize = (int)(len * sampleRate);
            int[] sampleIndices = new int[sampleSize];

            ReservoirSampler outlierSampler = new ReservoirSampler(sampleSize);
            ReservoirSampler inlierSampler = new ReservoirSampler(sampleSize);

            for (int i = 0; i < len; i++) {
                double curVal = metrics[i];
                if ((curVal > highCutoff && includeHigh)
                        || (curVal < lowCutoff && includeLow)
                        ) {
                    outlierSampler.process(i);
                } else {
                    inlierSampler.process(i);
                }
            }
            numOutliers = outlierSampler.getNumProcessed();

            int[] sampledOutlierIndices = outlierSampler.getSample();
            int[] sampledInlierIndices = inlierSampler.getSample();
            int outlierSampleSize = Math.min(sampleSize / 2, numOutliers);
            int idx = 0;
            for (int i = 0; i < outlierSampleSize; i++) {
                sampleIndices[idx++] = sampledOutlierIndices[i];
            }
            for (int i = 0; i < sampleSize - outlierSampleSize; i++) {
                sampleIndices[idx++] = sampledInlierIndices[i];
            }

            output = input.sample(sampleIndices);
            double[] resultColumn = new double[sampleSize];
            for (int i = 0; i < outlierSampleSize; i++) {
                resultColumn[i] = 1.0;
            }
            output.addColumn(outputColumnName, resultColumn);
            inlierWeight = ((double) (len - numOutliers) / (sampleSize - outlierSampleSize)) / ((double) numOutliers / outlierSampleSize);
            outlierSampleRate = (double) outlierSampleSize / numOutliers;
        } else {
            output = input.copy();
            double[] resultColumn = new double[len];
            for (int i = 0; i < len; i++) {
                double curVal = metrics[i];
                if ((curVal > highCutoff && includeHigh)
                        || (curVal < lowCutoff && includeLow)
                        ) {
                    resultColumn[i] = 1.0;
                }
            }
            output.addColumn(outputColumnName, resultColumn);
        }
        elapsed = System.nanoTime() - start;
        log.info("Classification time: {} ms", elapsed / 1.e6);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    // Parameter Getters and Setters
    public double getPercentile() {
        return percentile;
    }

    /**
     * @param percentile Cutoff point for high or low values
     * @return this
     */
    public PercentileClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        return this;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public PercentileClassifier setIncludeHigh(boolean includeHigh) {
        this.includeHigh = includeHigh;
        return this;
    }

    public boolean isIncludeLow() {
        return includeLow;
    }

    /**
     * @param includeLow Whether to count low points as outliers
     * @return this
     */
    public PercentileClassifier setIncludeLow(boolean includeLow) {
        this.includeLow = includeLow;
        return this;
    }

    public double getLowCutoff() {
        return lowCutoff;
    }

    public double getHighCutoff() {
        return highCutoff;
    }

    public void setSampleRate(double sampleRate) { this.sampleRate = sampleRate; }
    public void setOutlierSampleSize(int outlierSampleSize) { this.outlierSampleSize = outlierSampleSize; }
    public void setInlierSampleSize(int inlierSampleSize) { this.inlierSampleSize = inlierSampleSize; }
}

package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.RiskRatio;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;

import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.lang.Math;
import java.lang.Integer;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

public class MultiMADClassifierE2E implements Transformer {
    private double percentile = 0.5;
    private double zscore = 2.576;
    private boolean includeHigh = true;
    private boolean includeLow = true;
    private double samplingRate = 1;
    private List<String> columnNames;
    private String attributeName;
    private String outputColumnSuffix = "_OUTLIER";
    private boolean useParallel = false;
    private boolean useReservoirSampling = false;
    private boolean doGroupBy = false;
    private boolean doGroupByFast = false;
    private Map<String, MutableInt> attributeCounts;
    public Map<String, Integer> features;
    public Map<String, Double> ratios;
    private boolean usePercentile = false;
    private double minSupport = 0.002;
    private double minRiskRatio = 1;
    private final double trimmedMeanFallback = 0.05;
    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;
    public List<Explanation> explanations = new ArrayList<Explanation>();

    public MultiMADClassifierE2E(String attributeName, String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        this.attributeName = attributeName;
        this.features = new HashMap<String, Integer>();
        this.ratios = new HashMap<String, Double>();
    }

    @Override
    public void process(DataFrame input) {
        features = new HashMap<String, Integer>();
        ratios = new HashMap<String, Double>();

        output = input.copy();

        int sampleSize = (int)(input.getNumRows() * samplingRate);
        int[] sampleIndices = new int[sampleSize];
        if (samplingRate != 1) {
            sampleIndices = getRandomSample(input.getNumRows());
        }

        String[] attributes = input.getStringColumnByName(attributeName);
        if (doGroupByFast) {
            attributeCounts = new HashMap<String, MutableInt>();
            for (int r = 0; r < attributes.length; r++) {
                if (attributeCounts.containsKey(attributes[r])) {
                    attributeCounts.get(attributes[r]).increment();
                } else {
                    attributeCounts.put(attributes[r], new MutableInt(1));
                }
            }
        }
        
        for (String columnName : columnNames) {            
            double[] metrics = input.getDoubleColumnByName(columnName);

            if (usePercentile) {
                Percentile p = new Percentile();
                p.setData(metrics);
                lowCutoff = p.evaluate(percentile);
                highCutoff = p.evaluate(100.0 - percentile);
            } else {
                double[] trainingMetrics = new double[sampleSize];
                if (samplingRate == 1) {
                    System.arraycopy(metrics, 0, trainingMetrics, 0, metrics.length);
                } else {
                    for (int j = 0; j < sampleIndices.length; j++) {
                        trainingMetrics[j] = metrics[sampleIndices[j]];
                    }
                }

                Stats stats = train(trainingMetrics);
                lowCutoff = stats.median - (zscore * stats.mad);
                highCutoff = stats.median + (zscore * stats.mad);
            }

            if (useParallel) {
                // Parallelized stream scoring:
                double[] results = new double[metrics.length];
                System.arraycopy(metrics, 0, results, 0, metrics.length);
                Arrays.parallelSetAll(results, m -> (
                    (m > highCutoff && includeHigh) || (m < lowCutoff && includeLow)) ? 1.0 : 0.0);
                output.addDoubleColumn(columnName + outputColumnSuffix, results);
            } else {
                // Non-parallel scoring:
                if (doGroupBy) {
                    int numInliers = 0;
                    int numOutliers = 0;
                    Map<String, List<MutableInt>> counts = new HashMap<String, List<MutableInt>>();
                    double[] results = new double[metrics.length];
                    for (int r = 0; r < input.getNumRows(); r++) {
                        boolean isOutlier = (metrics[r] > highCutoff && includeHigh) ||
                            (metrics[r] < lowCutoff && includeLow);
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
                    output.addDoubleColumn(columnName + outputColumnSuffix, results);
                    long startTime = System.currentTimeMillis();
                    List<AttributeSet> attributeSets = new ArrayList<>();
                    int supportRequired = (int) (minSupport * numOutliers);
                    double topRatio = 0;
                    for (String attr : counts.keySet()) {
                        List<MutableInt> numInOut = counts.get(attr);
                        if (numInOut.get(0).get() < supportRequired) {
                            continue;
                        }
                        double ratio = RiskRatio.compute(numInOut.get(1).get(),
                            numInOut.get(0).get(),
                            numInliers,
                            numOutliers);
                        if (ratio > minRiskRatio) {
                            Map<String, String> items = new HashMap<String, String>();
                            items.put(attributeName, attr);
                            attributeSets.add(new AttributeSet(
                                numInOut.get(0).get() / (double) numOutliers,
                                numInOut.get(0).get(),
                                ratio,
                                items));
                        }
                        if (ratio > topRatio) {
                            topRatio = ratio;
                        }
                    }
                    ratios.put(columnName, topRatio);
                    features.put(columnName, attributeSets.size());
                    long elapsed = System.currentTimeMillis() - startTime;
                    Explanation explanation = new Explanation(
                        attributeSets,
                        numInliers,
                        numOutliers,
                        elapsed);
                    explanations.add(explanation);
                } else if (doGroupByFast) {
                    int numOutliers = 0;
                    Map<String, MutableInt> counts = new HashMap<String, MutableInt>();
                    double[] results = new double[metrics.length];
                    for (int r = 0; r < input.getNumRows(); r++) {
                        boolean isOutlier = (metrics[r] > highCutoff && includeHigh) ||
                            (metrics[r] < lowCutoff && includeLow);
                        results[r] = isOutlier ? 1.0 : 0.0;
                        if (isOutlier) {
                            numOutliers++;
                            if (counts.containsKey(attributes[r])) {
                                counts.get(attributes[r]).increment();
                            } else {
                                counts.put(attributes[r], new MutableInt(1));
                            }
                        }      
                    }
                    output.addDoubleColumn(columnName + outputColumnSuffix, results);
                    long startTime = System.currentTimeMillis();
                    List<AttributeSet> attributeSets = new ArrayList<>();
                    int supportRequired = (int) (minSupport * numOutliers);
                    double topRatio = 0;
                    for (String attr : counts.keySet()) {
                        int attrOutlierCount = counts.get(attr).get();
                        if (attrOutlierCount < supportRequired) {
                            continue;
                        }
                        double ratio = RiskRatio.compute(attributeCounts.get(attr).get() - attrOutlierCount,
                            attrOutlierCount,
                            metrics.length - numOutliers,
                            numOutliers);
                        if (ratio > minRiskRatio) {
                            Map<String, String> items = new HashMap<String, String>();
                            items.put(attributeName, attr);
                            attributeSets.add(new AttributeSet(
                                attrOutlierCount / (double) numOutliers,
                                attrOutlierCount,
                                ratio,
                                items));
                        }
                        if (ratio > topRatio) {
                            topRatio = ratio;
                        }
                    }
                    features.put(columnName, attributeSets.size());
                    ratios.put(columnName, topRatio);
                    long elapsed = System.currentTimeMillis() - startTime;
                    Explanation explanation = new Explanation(
                        attributeSets,
                        metrics.length - numOutliers,
                        numOutliers,
                        elapsed);
                    explanations.add(explanation);
                } else {
                    double[] results = new double[metrics.length];
                    for (int r = 0; r < input.getNumRows(); r++) {
                        boolean isOutlier = (metrics[r] > highCutoff && includeHigh) ||
                            (metrics[r] < lowCutoff && includeLow);
                        results[r] = isOutlier ? 1.0 : 0.0;
                    }
                    output.addDoubleColumn(columnName + outputColumnSuffix, results);
                }
            }
        }
    }

    private int[] getRandomSample(int numRows) {
        int sampleSize = (int)(numRows * samplingRate);
        int[] sampleIndices = new int[sampleSize];

        if (useReservoirSampling) {
            // Reservoir Sampling:
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
            boolean[] mask = new boolean[numRows];
            for (int i = 0; i < sampleSize; i++) {
                mask[sampleIndices[i]] = true;
            }
        } else {
            // partial Fisher-Yates shuffle
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
            System.arraycopy(range, 0, sampleIndices, 0, sampleSize);
        }

        return sampleIndices;
    }

    private Stats train(double[] metrics) {
        double median = new Percentile().evaluate(metrics, 50);

        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = Math.abs(metrics[i] - median);
        }

        double MAD = new Percentile().evaluate(metrics, 50);

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

        Stats stats = new Stats(median, MAD * MAD_TO_ZSCORE_COEFFICIENT);
        return stats;
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    // Parameter Getters and Setters
    public double getPercentile() {
        return percentile;
    }

    public double getZscore() {
        return zscore;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    public boolean isIncludeLow() {
        return includeLow;
    }

    public boolean isUseParallel() {
        return useParallel;
    }

    public boolean isUseReservoirSampling() {
        return useReservoirSampling;
    }

    public boolean isDoGroupBy() {
        return doGroupBy;
    }

    public boolean isDoGroupByFast() {
        return doGroupByFast;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getOutputColumnSuffix() {
        return outputColumnSuffix;
    }

    /**
     * @param percentile Approximate percent of data to classify as outlier on each end of
     * spectrum (i.e., a percentile of 5 means that the top 5% and bottom 5% are outliers)
     * @return this
     */
    public MultiMADClassifierE2E setPercentile(double percentile) {
        this.percentile = percentile;
        // https://en.wikipedia.org/wiki/Normal_distribution#Quantile_function
        this.zscore = Math.sqrt(2) * Erf.erfcInv(2.0*percentile/100.0);
        return this;
    }

    /**
     * @param zscore Z-score above/below which data is classified as outlier
     * @return this
     */
    public MultiMADClassifierE2E setZscore(double zscore) {
        this.zscore = zscore;
        return this;
    }

    /**
     * @param samplingRate Rate to sample elements used to calculate median and MAD, must be
     * greater than 0 and at most 1 (i.e., use all elements without sampling)
     * @return this
     */
    public MultiMADClassifierE2E setSamplingRate(double samplingRate) {
        this.samplingRate = samplingRate;
        return this;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public MultiMADClassifierE2E setIncludeHigh(boolean includeHigh) {
        this.includeHigh = includeHigh;
        return this;
    }
    
    /**
     * @param includeLow Whether to count low points as outliers
     * @return this
     */
    public MultiMADClassifierE2E setIncludeLow(boolean includeLow) {
        this.includeLow = includeLow;
        return this;
    }

    /**
     * @param useParallel Whether to use parallelization, can be beneficial for large datasets
     * @return this
     */
    public MultiMADClassifierE2E setUseParallel(boolean useParallel) {
        this.useParallel = useParallel;
        return this;
    }

    /**
     * @param useParallel Whether to do group-by on attributes using metrics
     * @return this
     */
    public MultiMADClassifierE2E setDoGroupBy(boolean doGroupBy) {
        this.doGroupBy = doGroupBy;
        return this;
    }

    public MultiMADClassifierE2E setDoGroupByFast(boolean doGroupByFast) {
        this.doGroupByFast = doGroupByFast;
        return this;
    }

    /**
     * @param use ReservoirSampling Whether to sample using reservoir sampling. Otherwise,
     * a partial Fisher-Yates shuffle is used.
     * @return this
     */
    public MultiMADClassifierE2E setUseReservoirSampling(boolean useReservoirSampling) {
        this.useReservoirSampling = useReservoirSampling;
        return this;
    }

    public MultiMADClassifierE2E setUsePercentile(boolean usePercentile) {
        this.usePercentile = usePercentile;
        return this;
    }

    public MultiMADClassifierE2E setColumnNames(String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        return this;
    }

    /**
     * @param outputColumnSuffix The output for column "a" would be in a column named
     * "a" + outputColumnSuffix.
     * @return this
     */
    public MultiMADClassifierE2E setOutputColumnSuffix(String outputColumnSuffix) {
        this.outputColumnSuffix = outputColumnSuffix;
        return this;
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

    private final class MutableInt {
        private int value;
        public MutableInt(int value) {
            this.value = value;
        }
        public void increment() { ++value;      }
        public int  get()       { return value; }
    }
}

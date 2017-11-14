package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.Math;
import java.lang.Double;

import java.util.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class MultiMADOptimizationTest {
    public static DateFormat minute = new SimpleDateFormat("HH_mm");
    Date date;

    private String inputFileName, attributeName;
    private String[] columnNames;
    private int numTrials;
    private Integer[] samplingRates;

    private double totalTime, trainTime, scoreTime, samplingTime, bootstrapTime;
    private double medCISize, MADCISize, medError, MADError;

    private List<String> tLines, eLines;
    private String timingFile;
    private String estimateFile;

    private DataFrame df;
    private static List<Double> trueMedians, trueMADs;
    private double percentOutliers;
    private long startTime, estimatedTime;
    private MultiMADClassifierDebug mad;

    @Before
    public void setUp() throws Exception {
        date = new Date();
        inputFileName = System.getProperty("input");
        columnNames = System.getProperty("metrics").split(",");
        attributeName = System.getProperty("attr");
        numTrials = Integer.valueOf(System.getProperty("trials"));
        timingFile = String.format("multiMAD/timing/%s_%s.csv", minute.format(date), System.getProperty("dataset"));//,Arrays.toString(samplingRates));
        estimateFile = String.format("multiMAD/errors/%s_%s.csv", minute.format(date), System.getProperty("dataset"));

        samplingRates = new Integer[]{2,10,100};
        percentOutliers = 0.1;
        startTime = 0;
        estimatedTime = 0;

        Map<String, Schema.ColType> schema = new HashMap<>();
        for (String c: columnNames) {
            schema.put(c, Schema.ColType.DOUBLE);
        }
        schema.put(attributeName, Schema.ColType.DOUBLE);
        DataFrameLoader loader = new CSVDataFrameLoader(inputFileName).setColumnTypes(schema);
        df = loader.load();

        tLines = new ArrayList<>();
        eLines = new ArrayList<>();
        tLines.add("total, train, score, sampling, bootstrap");
        eLines.add("medCI, MADCI, medError, MADError");
        System.out.println("Done Setting Up");

    }

    @Test
    public void warmUp() throws Exception {
        mad = new MultiMADClassifierDebug(attributeName, columnNames)
                .setPercentile(percentOutliers);

        //TURNING OFF BOOTSTRAP TEMPORARILY FOR TIME CALC
        mad.doBootstrap = false;

        for (int i = 0; i < 2; i++) {
            mad.process(df);
        }
        System.out.println("Done Warming Up");
    }

    @Test
    public void testBenchmarkAndSampling() throws Exception {
        System.gc();
        startTime = System.currentTimeMillis();

        mad = new MultiMADClassifierDebug(attributeName, columnNames)
                .setPercentile(percentOutliers);
        for (int i = 0; i < numTrials; i++) {
            mad.process(df);
        }
        trueMedians = mad.getMedians();
        trueMADs = mad.getMADs();

        estimatedTime = System.currentTimeMillis() - startTime;
        totalTime = (estimatedTime - mad.getOtherTime())/((double)numTrials);
        trainTime = mad.getTrainTime()/((double)numTrials);
        scoreTime = mad.getScoreTime()/((double)numTrials);
        samplingTime = mad.getSamplingTime()/((double)numTrials);
        bootstrapTime = mad.getOtherTime()/((double)numTrials);

        System.out.format("Unoptimized avg time elapsed: %f ms\n", totalTime);
        System.out.format("train: %f ms, score: %f ms, sampling: %f ms, bootstrap (not counted): %f ms\n",
                trainTime, scoreTime, samplingTime, bootstrapTime);
        System.out.println("");
        tLines.add(String.valueOf(totalTime)+","+String.valueOf(trainTime)+","
                +String.valueOf(scoreTime)+","
                +String.valueOf(samplingTime)+","
                +String.valueOf(bootstrapTime));

        for (int rate: samplingRates){
            samplingRun(rate);
        }
    }

    public void samplingRun(int samplingRate) {
        System.gc();
        startTime = System.currentTimeMillis();

        mad = new MultiMADClassifierDebug(attributeName, columnNames)
                .setPercentile(percentOutliers)
                .setSamplingRate(samplingRate);
        for (int i = 0; i < numTrials; i++) {
            mad.process(df);
        }

        //TURNING OFF BOOTSTRAP TEMPORARILY FOR TIME CALC
        mad.doBootstrap = false;

        estimatedTime = System.currentTimeMillis() - startTime;
        totalTime = (estimatedTime - mad.getOtherTime())/((double)numTrials);
        trainTime = mad.getTrainTime()/((double)numTrials);
        scoreTime = mad.getScoreTime()/((double)numTrials);
        samplingTime = mad.getSamplingTime()/((double)numTrials);
        bootstrapTime = mad.getOtherTime()/((double)numTrials);

        System.out.format("Sampling (1/%d of elements) time elapsed: %f ms\n",
                samplingRate, totalTime);
        System.out.format("train: %f ms, score: %f ms, sampling: %f ms, boostrap (not counted): %f ms\n",
                trainTime, scoreTime, samplingTime, bootstrapTime);
        tLines.add(String.valueOf(totalTime)+","
                +String.valueOf(trainTime)+","
                +String.valueOf(scoreTime)+","
                +String.valueOf(samplingTime)+","
                +String.valueOf(bootstrapTime));

        if (mad.doBootstrap) {
            List<Double> medians = mad.getMedians();
            List<Double> MADs = mad.getMADs();
            List<Double> upperBoundsMedian = mad.upperBoundsMedian;
            List<Double> lowerBoundsMedian = mad.lowerBoundsMedian;
            List<Double> upperBoundsMAD = mad.upperBoundsMAD;
            List<Double> lowerBoundsMAD = mad.lowerBoundsMAD;
            double med_sum = 0;
            double mad_sum = 0;
            double med_err_sum = 0;
            double mad_err_sum = 0;
            int num_metrics = columnNames.length;
            for (int i = 0; i < num_metrics; i++) {
                for (int t = 0; t < numTrials; t++) {
                    int idx = t * num_metrics + i;
                    double medianError = Math.abs(medians.get(idx) - trueMedians.get(i)) / trueMADs.get(i);
                    double MADError = Math.abs(MADs.get(idx) - trueMADs.get(i)) / trueMADs.get(i);
                    double medianCItoMAD = (upperBoundsMedian.get(idx) - lowerBoundsMedian.get(i)) / trueMADs.get(i);
                    double madCItoMAD = (upperBoundsMAD.get(idx) - lowerBoundsMAD.get(i)) / trueMADs.get(i);
                    med_sum += medianCItoMAD;
                    mad_sum += madCItoMAD;
                    med_err_sum += medianError;
                    mad_err_sum += MADError;
                }
            }

            medCISize = med_sum / (double) (num_metrics * numTrials);
            MADCISize = mad_sum / (double) (num_metrics * numTrials);
            medError = med_err_sum / (double) (num_metrics * numTrials);
            MADError = mad_err_sum / (double) (num_metrics * numTrials);

            System.out.format("Avg median 95%% CI size: %f, avg MAD 95%% CI size: %f, avg median error: %f, avg MAD error: %f\n",
                medCISize, MADCISize, medError, MADError);
            System.out.println("");
            eLines.add(String.valueOf(medCISize)+","
                    +String.valueOf(MADCISize)+","
                    +String.valueOf(medError)+","
                    +String.valueOf(MADError));
        }
    }

    @After
    public void dumpToCSV(){
        arrayListToCSV(tLines,timingFile);
        arrayListToCSV(eLines,estimateFile);
    }

    public void arrayListToCSV(List<String> data, String path) {
        File f = new File(path);
        f.getParentFile().mkdirs();
        String eol =  System.getProperty("line.separator");
        try (Writer writer = new FileWriter(f)) {
            for (String entry: data) {
                writer.append(entry);
                writer.append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }
}

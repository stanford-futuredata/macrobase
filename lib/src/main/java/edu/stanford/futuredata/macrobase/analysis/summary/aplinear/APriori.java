package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

public abstract class APriori {
    public double shardTime = 0;
    public double initializationTime = 0;
    public double rowstoreTime = 0;
    public double[] explainTime = {0, 0, 0};
    public double[] aggregationTime = {0, 0, 0};
    public double[] pruneTime = {0, 0, 0};
    public double[] saveTime = {0, 0, 0};
    public int[] numProcessed = {0, 0, 0};
    public int[] numSaved = {0, 0, 0};
    public int[] numNext = {0, 0, 0};
}

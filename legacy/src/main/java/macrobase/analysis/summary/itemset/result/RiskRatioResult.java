package macrobase.analysis.summary.itemset.result;

public class RiskRatioResult {
    private double rr = 0;
    private double correction = 0;

    public RiskRatioResult(double rr, double correction) {
        this.rr = rr;
        this.correction = correction;
    }

    public RiskRatioResult(double rr) {
        this.rr = rr;
    }

    public double get() { return rr; }

    public double getCorrected() { return correction; }

    public double getCorrectedRiskRatio() { return rr - correction; }
}

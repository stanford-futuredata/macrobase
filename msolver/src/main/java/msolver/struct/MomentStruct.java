package msolver.struct;

public class MomentStruct {
    public double min, max, logMin, logMax;
    public double[] powerSums;
    public double[] logSums;

    public MomentStruct() {
        this.min = 0;
        this.max = 1;
        this.powerSums = new double[]{1.0};
        this.logMin = 0;
        this.logMax = 1;
        this.logSums = new double[]{1.0};
    }

    public MomentStruct(
            double min, double max, double[] powerSums,
            double logMin, double logMax, double[] logSums
    ) {
        this.min = min;
        this.max = max;
        this.powerSums = powerSums;
        this.logMin = logMin;
        this.logMax = logMax;
        this.logSums = logSums;
    }
}

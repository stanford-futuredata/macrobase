package msolver.data;

import java.util.Arrays;

public abstract class MomentData {
    abstract public double[] getPowerSums();
    abstract public double getMin();
    abstract public double getMax();

    public double[] getLogSums() {
        double[] results = new double[1];
        results[0] = 1.0;
        return results;
    }
    public double getLogMin() {
        return 0.0;
    }
    public double getLogMax() {
        return 0.0;
    }

    public double[] getPowerSums(int k) {
        return Arrays.copyOf(getPowerSums(), k);
    }
    public double[] getLogSums(int k) {
        return Arrays.copyOf(getLogSums(), k);
    }
}

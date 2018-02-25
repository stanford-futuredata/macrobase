package msolver.data;

public class UniformData extends MomentData{
    public static final int N = 1000000;
    public static final int k = 100;

    @Override
    public double[] getPowerSums() {
        double[] powerSums = new double[k];
        for (int i = 0; i < k; i++) {
            powerSums[i] = N*1.0 / (i+1);
        }
        return powerSums;
    }

    @Override
    public double getMin() {
        return 0.0;
    }

    @Override
    public double getMax() {
        return 1.0;
    }
}

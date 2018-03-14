package msolver.util;

import org.jtransforms.fft.DoubleFFT_1D;

public class FastDCT {
    private int n;
    private double[] y;
    private double[] out;
    public FastDCT(int n) {
        this.n = n;
        y = new double[2*n-2];
        out = new double[n];
    }
    public double[] dct(double[] x) {
        System.arraycopy(x, 0, y, 0, n);
        for (int i = n; i < y.length; i++) {
            y[i] = x[2*n-2-i];
        }
        DoubleFFT_1D fft = new DoubleFFT_1D(y.length);
        fft.realForward(y);

        for (int i = 0; i < n-1; i++) {
            out[i] = y[2*i]/2;
        }
        out[n-1] = y[1];
        return out;
    }
}

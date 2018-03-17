package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;

/**
 * Performs linear interpolation in a lazy manner: interpolation does not actually
 * occur until an evaluation is requested.
 */
public class LinearInterpolator {
    private double[] x;
    private double[] y;

    /**
     * @param x Should be sorted in non-descending order. Assumed to not be very large.
     */
    public LinearInterpolator(double[] x, double[] y) throws IllegalArgumentException {
        if (x.length != y.length) {
            throw new IllegalArgumentException("X and Y must be the same length");
        }
        if (x.length == 1) {
            throw new IllegalArgumentException("X must contain more than one value");
        }
        this.x = x;
        this.y = y;
    }

    public double evaluate(double value) throws MacroBaseInternalError {
        if ((value > x[x.length - 1]) || (value < x[0])) {
            return Double.NaN;
        }

        for (int i = 0; i < x.length; i++) {
            if (value == x[i]) {
                return y[i];
            }
            if (value >= x[i+1]) {
                continue;
            }
            double dx = x[i+1] - x[i];
            double dy = y[i+1] - y[i];
            double slope = dy / dx;
            double intercept = y[i] - x[i] * slope;
            return slope * value + intercept;
        }

        throw new MacroBaseInternalError("Linear interpolator implemented incorrectly");
    }
}

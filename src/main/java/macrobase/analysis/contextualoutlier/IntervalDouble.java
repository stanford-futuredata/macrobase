package macrobase.analysis.contextualoutlier;

import macrobase.ingest.DatumEncoder;

public class IntervalDouble extends Interval {

    double min;
    double max;

    public IntervalDouble(int dimension, String columnName, double min, double max) {
        super(dimension, columnName);
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    @Override
    public String print(DatumEncoder encoder) {
        return "< " + columnName + " " + '\u2208' + " [" + min + "," + max + ") > ";
    }

    @Override
    public boolean contains(Object d) {
        double dd = (double) d;
        if (dd >= min && dd < max)
            return true;
        else
            return false;
    }

    @Override
    public String toString() {
        return columnName + " in [ " + min + " , " + max + " )";
    }
}

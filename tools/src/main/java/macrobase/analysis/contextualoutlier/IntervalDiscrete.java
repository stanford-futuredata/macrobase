package macrobase.analysis.contextualoutlier;

import macrobase.ingest.DatumEncoder;

public class IntervalDiscrete extends Interval {

    int value; //the integer used to encode the value

    public IntervalDiscrete(int dimension, String columnName, int value) {
        super(dimension, columnName);
        this.value = value;
    }

    @Override
    public boolean contains(Object d) {
        int dd = (int) d;
        if (dd == value)
            return true;
        else
            return false;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String print(DatumEncoder encoder) {
        return "< " + columnName + " = " + encoder.getAttribute(value).getValue() + " > ";
    }

    @Override
    public String toString() {
        return columnName + " = " + value;
    }
}

package macrobase.datamodel;

import java.util.Comparator;

public class DatumComparator implements Comparator {

    private int dimension;

    /**
     * Create a Comparator subclass that compares Datums based on
     * dimension-th dimension in the metrics of datum object.
     * @param dimension  dimension to use for comparison
     */
    public DatumComparator(int dimension) {
        this.dimension = dimension;
    }

    @Override
    public int compare(Object o1, Object o2) {
        Datum d1 = (Datum) o1;
        Datum d2 = (Datum) o2;
        Double metric1 = d1.metrics().getEntry(this.dimension);
        Double metric2 = d2.metrics().getEntry(this.dimension);
        return metric1.compareTo(metric2);
    }
}

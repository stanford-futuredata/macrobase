package macrobase.analysis.result;

import macrobase.datamodel.Datum;

public class DatumWithNorm {
    private final Datum datum;
    private final double norm;

    public Double getNorm() {
        return norm;
    }

    public Datum getDatum() {
        return datum;
    }

    public DatumWithNorm(Datum d) {
        this.datum = d;
        this.norm = d.getMetrics().getNorm();
    }

    public DatumWithNorm(Datum d, double norm) {
        this.datum = d;
        this.norm = norm;
    }
}

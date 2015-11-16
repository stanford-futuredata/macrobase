package macrobase.analysis.outlier.result;

import macrobase.datamodel.Datum;

public class DatumWithScore {
    private Datum datum;
    private double score;

    public DatumWithScore(Datum datum, double score) {
        this.datum = datum;
        this.score = score;
    }

    public Datum getDatum() {
        return datum;
    }

    public double getScore() {
        return score;
    }
}

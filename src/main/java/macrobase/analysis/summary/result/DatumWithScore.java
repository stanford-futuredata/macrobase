package macrobase.analysis.summary.result;

import macrobase.datamodel.Datum;

// TODO: REMOVE after we refactor contextual outlier detection not to depend on
//       DatumWithScore
public class DatumWithScore {
    private Datum datum;
    private Double score;

    public DatumWithScore(Datum datum, double score) {
        this.datum = datum;
        this.score = score;
    }

    public Datum getDatum() {
        return datum;
    }

    public Double getScore() {
        return score;
    }
}

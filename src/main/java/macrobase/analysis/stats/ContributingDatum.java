package macrobase.analysis.stats;

import macrobase.datamodel.Datum;

// hack class used to hold Garthwaite-Koch contributions in MCD
public class ContributingDatum extends Datum {
    public ContributingDatum(Datum d, double score, double[] contribution) {
        super(d, score);
        this.contribution = contribution;
    }

    public double[] contribution;
}

package macrobase.analysis.result;

import macrobase.datamodel.Datum;

public class OutlierClassificationResult {

    private Boolean isClassifiedAsOutlier;
    private Datum datum;

    public OutlierClassificationResult(Datum datum, Boolean outlier) {
        this.isClassifiedAsOutlier = outlier;
        this.datum = datum;
    }

    public Boolean isOutlier() {
        return isClassifiedAsOutlier;
    }

    public Datum getDatum() {
        return datum;
    }
}

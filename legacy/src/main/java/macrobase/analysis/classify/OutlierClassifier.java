package macrobase.analysis.classify;

import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.result.OutlierClassificationResult;

import macrobase.datamodel.Datum;

public abstract class OutlierClassifier extends MBOperator<Datum, OutlierClassificationResult> {
    protected class DatumWithNorm {
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
            this.norm = d.norm();
        }

        public DatumWithNorm(Datum d, double norm) {
            this.datum = d;
            this.norm = norm;
        }
    }
}

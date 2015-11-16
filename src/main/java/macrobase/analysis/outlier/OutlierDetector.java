package macrobase.analysis.outlier;

import macrobase.analysis.outlier.result.DatumWithScore;
import macrobase.datamodel.Datum;

import java.util.List;

/**
 * Created by pbailis on 12/14/15.
 */
public interface OutlierDetector {
    class BatchResult {
        private List<DatumWithScore> inliers;
        private List<DatumWithScore> outliers;

        public BatchResult(List<DatumWithScore> inliers, List<DatumWithScore> outliers) {
            this.inliers = inliers;
            this.outliers = outliers;
        }

        public List<DatumWithScore> getInliers() {
            return inliers;
        }

        public List<DatumWithScore> getOutliers() {
            return outliers;
        }
    }

    public BatchResult classifyBatch(List<Datum> data);
}

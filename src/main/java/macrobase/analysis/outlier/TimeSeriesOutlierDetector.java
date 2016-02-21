package macrobase.analysis.outlier;

import java.util.List;

import macrobase.datamodel.Datum;

public abstract class TimeSeriesOutlierDetector extends OutlierDetector {
    protected final int tupleWindowSize;
    private int currentTupleWindowSize;

    public TimeSeriesOutlierDetector(int tupleWindowSize) {
        assert tupleWindowSize > 1;
        this.tupleWindowSize = tupleWindowSize;
    }

    public abstract void addToWindow(Datum datum);

    public abstract void removeLastFromWindow();

    // Score current window.
    public abstract double scoreWindow();

    @Override
    public void train(List<Datum> data) {
        // Just sanity checks - we don't actually compute anything in train,
        // since we train as we go while scoring.
        assert data.size() >= tupleWindowSize;
        assert data.get(0).getTime() != null;
    }

    @Override
    public double score(Datum datum) {
        /*
         * Note: we assume score is called with data in order - this is true for
         * batch analysis. Somewhat hacky but allows us to maintain
         * compatibility with OutlierDetector.
         */
        if (currentTupleWindowSize == tupleWindowSize) {
            currentTupleWindowSize--;
            removeLastFromWindow();
        }
        currentTupleWindowSize++;
        addToWindow(datum);
        return scoreWindow();
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        return 0;
    }

}

package macrobase.analysis.stats;

import java.util.List;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

public abstract class TimeSeriesScore extends BatchTrainScore {
    protected final int tupleWindowSize;
    protected final Integer timeColumn;
    private int currentTupleWindowSize;

    public TimeSeriesScore(MacroBaseConf conf) {
        super(conf);
        this.tupleWindowSize = conf.getInt(MacroBaseConf.TUPLE_WINDOW, MacroBaseDefaults.TUPLE_WINDOW);
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        assert tupleWindowSize > 1;
    }

    public abstract void addToWindow(Datum datum);

    public abstract void removeLastFromWindow();

    /**
     * Use the current window to build a model and then score the latest datum.
     */
    public abstract double scoreWindow();

    @Override
    public void train(List<Datum> data) {
        // Just sanity checks - we don't actually compute anything in train,
        // since we train as we go while scoring.
        assert data.size() >= tupleWindowSize;
        assert timeColumn != null;
    }

    @Override
    public double score(Datum datum) {
        /*
         * Note: we assume score is called with data in order - this is true for
         * batch analysis. Somewhat hacky but allows us to maintain
         * compatibility with BatchTrainScore.
         */
        if (currentTupleWindowSize == tupleWindowSize) {
            currentTupleWindowSize--;
            removeLastFromWindow();
        }
        currentTupleWindowSize++;
        addToWindow(datum);
        return scoreWindow();
    }
}

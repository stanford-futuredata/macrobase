package macrobase.analysis.transform.aggregate;

import macrobase.datamodel.Datum;

import java.util.List;

public abstract class BatchedWindowAggregate implements WindowAggregate {
    protected Integer timeColumn = null;
    protected Datum currWindow;

    public void reset() {currWindow = null;}

    /* Whether the class supports sub-aggregation (panes) */
    public abstract boolean paneEnabled();

    public abstract Datum aggregate(List<Datum> data);
}

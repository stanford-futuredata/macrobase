package macrobase.analysis.transform.aggregate;

import macrobase.datamodel.Datum;
import java.util.List;

public abstract class BatchedWindowAggregate implements IncrementalWindowAggregate {
    protected Integer timeColumn = null;

    /* Placeholder for naive implementations of aggregates */
    public Datum process(List<Datum> data) {return null;}
}


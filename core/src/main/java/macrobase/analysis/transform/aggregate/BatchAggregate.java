package macrobase.analysis.transform.aggregate;

import macrobase.datamodel.Datum;

import java.util.List;

public interface BatchAggregate {
    Datum aggregate(List<Datum> data);
}

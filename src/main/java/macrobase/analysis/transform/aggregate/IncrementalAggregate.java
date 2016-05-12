package macrobase.analysis.transform.aggregate;

import macrobase.datamodel.Datum;

import java.util.List;

public interface IncrementalAggregate {
    Datum updateWindow(List<Datum> new_data, List<Datum> old_data);
}

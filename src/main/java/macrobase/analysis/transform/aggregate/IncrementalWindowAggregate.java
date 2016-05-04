package macrobase.analysis.transform.aggregate;

import macrobase.datamodel.Datum;
import java.util.List;

public interface IncrementalWindowAggregate {
    Datum process(List<Datum> new_data, List<Datum> expired_data);
}

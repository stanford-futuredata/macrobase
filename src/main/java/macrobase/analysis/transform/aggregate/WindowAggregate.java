package macrobase.analysis.transform.aggregate;

import macrobase.datamodel.Datum;
import java.util.List;

public interface WindowAggregate {
    Datum updateWindow(List<Datum> new_data, List<Datum> expired_data);
}

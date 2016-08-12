package macrobase.analysis.transform.aggregate;

import macrobase.datamodel.Datum;

import java.util.List;

public interface PaneAggregate {
    Datum updateWindow(List<Datum> new_panes, List<Datum> old_panes);
    Datum superAggregate(List<Datum> panes);
}

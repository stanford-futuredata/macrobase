package macrobase.analysis.transform.aggregate;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.List;

public class IncrementalWindowAggregate implements IncrementalAggregate {
    protected Datum currWindow;
    protected Integer timeColumn;
    protected int dim = 0;

    public IncrementalWindowAggregate() {}

    public IncrementalWindowAggregate(MacroBaseConf conf) throws ConfigurationException {
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
    }

    public Datum updateWindow(List<Datum> new_data, List<Datum> old_data) { return null; }
}

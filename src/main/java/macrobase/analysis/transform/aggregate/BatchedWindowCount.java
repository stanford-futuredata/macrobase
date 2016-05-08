package macrobase.analysis.transform.aggregate;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;

public class BatchedWindowCount extends BatchedWindowAggregate {
    public BatchedWindowCount() {}

    public BatchedWindowCount(MacroBaseConf conf) {
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
    }

    public boolean paneEnabled() {return false;}

    private int getCount() {
        for (int i = 0; i < currWindow.getMetrics().getDimension(); i ++)
            if (timeColumn == null || i != timeColumn)
                return (int) currWindow.getMetrics().getEntry(i);
        return 0;
    }

    public Datum updateWindow(List<Datum> new_data, List<Datum> expired_data) {
        if (currWindow == null) { // first window
            currWindow = aggregate(new_data);
        } else { // update existing window
            int dim = new_data.get(0).getMetrics().getDimension();
            RealVector results = new ArrayRealVector(dim);
            int new_count = getCount() + new_data.size() - expired_data.size();
            for (int i = 0; i < dim; i ++) {
                results.setEntry(i, new_count);
            }
            currWindow = new Datum(new ArrayList<>(), results);
        }
        return currWindow;
    }

    public Datum aggregate(List<Datum> data) {
        int dim = data.get(0).getMetrics().getDimension();
        RealVector results = new ArrayRealVector(dim);
        for (int i = 0; i < dim; i ++) {
            results.setEntry(i, data.size());
        }

        return new Datum(new ArrayList<>(), results);
    }
}
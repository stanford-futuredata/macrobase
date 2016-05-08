package macrobase.analysis.transform.aggregate;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;


public class BatchedWindowSum extends BatchedWindowAggregate {
    public BatchedWindowSum() {}

    public BatchedWindowSum(MacroBaseConf conf) throws ConfigurationException {
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
    }

    public boolean paneEnabled() {return true;}

    private Datum negate(Datum data) {
        RealVector metrics = data.getMetrics();
        int dim = metrics.getDimension();
        for (int i = 0; i < dim; i ++) {
            if (timeColumn != null && i == timeColumn)
                continue;
            data.getMetrics().setEntry(i, -metrics.getEntry(i));
        }
        return data;
    }

    public Datum updateWindow(List<Datum> new_panes, List<Datum> expired_panes) {
        if (currWindow == null) { // first window
            currWindow = aggregate(new_panes);
        } else { // update existing window
            List<Datum> updates = new ArrayList<>();
            updates.add(currWindow);
            updates.add(aggregate(new_panes));
            updates.add(negate(aggregate(expired_panes)));
            currWindow = aggregate(updates);
        }
        return currWindow;
    }

    @Override
    public Datum aggregate(List<Datum> data) {
        int dim = data.get(0).getMetrics().getDimension();
        RealVector results = new ArrayRealVector(dim);

        for (Datum d : data) {
            RealVector metrics = d.getMetrics();
            for (int i = 0; i < dim; i ++) {
                if (timeColumn != null && i == timeColumn)
                    continue;
                results.setEntry(i, results.getEntry(i) + metrics.getEntry(i));
            }
        }

        return new Datum(new ArrayList<>(), results);
    }
}

package macrobase.analysis.transform.aggregate;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;

public class IncrementalWindowCount extends IncrementalWindowAggregate {
    private int count = 0;

    public IncrementalWindowCount(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
    }

    public Datum updateWindow(List<Datum> new_data, List<Datum> old_data) {
        if (new_data.isEmpty() && old_data.isEmpty())
            return currWindow;

        if (currWindow == null) {
            dim = new_data.get(0).metrics().getDimension();
        }
        count += new_data.size() - old_data.size();
        RealVector results = new ArrayRealVector(dim);
        results.set(count);
        currWindow = new Datum(new ArrayList<>(), results);
        return currWindow;
    }
}

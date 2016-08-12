package macrobase.analysis.transform.aggregate;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.List;

public class BatchWindowMax extends BatchWindowAggregate {
    public BatchWindowMax(){}

    public BatchWindowMax(MacroBaseConf conf) throws ConfigurationException {
        this.timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
    }

    public Datum aggregate(List<Datum> data) {
        if (data.isEmpty())
            return new Datum(new ArrayList<>(), new ArrayRealVector(dim));

        dim = data.get(0).metrics().getDimension();
        RealVector results = new ArrayRealVector(dim);
        results.set(Double.MIN_VALUE);

        for (Datum d : data) {
            RealVector metrics = d.metrics();
            for (int i = 0; i < dim; i ++) {
                if (timeColumn != null && i == timeColumn)
                    continue;
                results.setEntry(i, Math.max(results.getEntry(i), metrics.getEntry(i)));
            }
        }

        return new Datum(new ArrayList<>(), results);
    }
}

package macrobase.analysis.stats.kde;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TKDE extends BatchTrainScore {
    public TKDEConf tConf;
    public TKDEEstimator estimator;

    public TKDE(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
        try {
            tConf = TKDEConf.parseYAML(conf.getSubConfString(TKDEConf.SUBKEY));
        } catch (IOException e) {
            throw new ConfigurationException(e.getMessage());
        }
        estimator = new TKDEEstimator(tConf);
    }


    @Override
    public void train(List<Datum> data) {
        List<double[]> metrics = new ArrayList<>(data.size());

        for (Datum d : data) {
            ArrayRealVector vec = (ArrayRealVector)d.metrics();
            metrics.add(vec.getDataRef());
        }
        estimator.train(metrics);
    }

    @Override
    public double score(Datum datum) {
        double curDensity = estimator.density(datum.metrics().toArray());
        // Return positive effective "outlier distance"
        return 1.0/curDensity;
    }
}

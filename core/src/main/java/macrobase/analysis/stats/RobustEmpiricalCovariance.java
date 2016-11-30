package macrobase.analysis.stats;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public class RobustEmpiricalCovariance extends BatchTrainScore {
    public Winsorizer trimmer;
    public Gaussian gModel;

    public static final String CONF_TRIM_PCT = "macrobase.analysis.rcov.trimPercent";
    public static final double CONF_TRIM_PCT_DEFAULT = 5.0;

    public RobustEmpiricalCovariance(MacroBaseConf conf) {
        super(conf);
        double trimPct = conf.getDouble(CONF_TRIM_PCT, CONF_TRIM_PCT_DEFAULT);
        trimmer = new Winsorizer(trimPct);
    }

    @Override
    public void train(List<Datum> data) {
        int k = data.get(0).metrics().getDimension();
        int n = data.size();
        List<double[]> metrics = new ArrayList<>(n);
        for (Datum curDatum : data) {
            metrics.add(curDatum.metrics().toArray());
        }
        List<double[]> trimmedMetrics = trimmer.process(metrics);

        gModel = new Gaussian().fit(trimmedMetrics);
    }

    @Override
    public double score(Datum datum) {
        return gModel.mahalanobis(datum.metrics().toArray());
    }
}

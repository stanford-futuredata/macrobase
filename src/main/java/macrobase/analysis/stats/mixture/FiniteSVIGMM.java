package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.List;

public class FiniteSVIGMM extends VarGMM {

    public FiniteSVIGMM(MacroBaseConf conf) {
        super(conf);
    }

    @Override
    protected double[] getNormClusterContrib() {
        return new double[0];
    }

    @Override
    public double[] getClusterProportions() {
        return new double[0];
    }

    @Override
    public double[] getClusterProbabilities(Datum d) {
        return new double[0];
    }

    @Override
    public void train(List<Datum> data) {

    }
}

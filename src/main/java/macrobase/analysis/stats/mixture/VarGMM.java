package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.util.List;

public abstract class VarGMM extends BatchMixtureModel {
    protected NormalWishartClusters clusters;
    protected List<MultivariateTDistribution> predictiveDistributions;

    public VarGMM(MacroBaseConf conf) {
        super(conf);
    }

    //public abstract double calculateLogLikelihood(List<Datum> data, MixingComponents mixingComponents, NormalWishartClusters clusters);

    @Override
    public List<RealMatrix> getClusterCovariances() {
        return clusters.getMAPCovariances();
    }

    @Override
    public List<RealVector> getClusterCenters() {
        return clusters.getMAPLocations();
    }

    public double calculateLogLikelihood(List<Datum> data, MixingComponents mixingComonents, NormalWishartClusters clusters) {
        predictiveDistributions = clusters.constructPredictiveDistributions();
        double logLikelihood = 0;
        for (Datum d : data) {
            logLikelihood += score(d);
        }
        return logLikelihood;
    }

}

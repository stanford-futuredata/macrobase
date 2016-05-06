package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.util.List;

public abstract class MeanFieldGMM extends BatchMixtureModel {
    protected double progressCutoff;

    // Parameters for Base Distribution, which is Wishart-Gaussian
    protected double baseNu;
    protected RealMatrix baseOmega;
    protected RealMatrix baseOmegaInverse;  // Use inverse of baseOmega, since it is used in the update equations.
    protected double baseBeta;
    protected RealVector baseLoc;

    // Variables governing atoms (components).
    // Omega and atomDOF for Wishart distribution for the precision matrix of the clusters.
    protected double atomDOF[];
    protected List<RealMatrix> atomOmega;
    // Parameters for Normal distribution for atom position, N(atomLocation, (atomBeta * Lambda))
    // where Lambda is Wishart distributed given parameters above.
    protected double atomBeta[];
    protected List<RealVector> atomLoc;


    public MeanFieldGMM(MacroBaseConf conf) {
        super(conf);
        this.progressCutoff = conf.getDouble(MacroBaseConf.EM_CUTOFF_PROGRESS, MacroBaseDefaults.EM_CUTOFF_PROGRESS);
    }
}

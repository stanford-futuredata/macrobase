package macrobase.analysis.pipeline;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.MAD;
import macrobase.analysis.stats.MinCovDet;
import macrobase.analysis.stats.RobustEmpiricalCovariance;
import macrobase.analysis.stats.kde.TKDE;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

import static macrobase.conf.MacroBaseConf.TRANSFORM_TYPE;

public class BatchTransformFactory {
    public static BatchTrainScore getTransform(
            MacroBaseConf conf
    ) throws ConfigurationException{
        String transformType = conf.getString(TRANSFORM_TYPE, "RCOV")
                .trim().toUpperCase();
        switch (transformType) {
            case "RCOV":
                return new RobustEmpiricalCovariance(conf);
            case "MAD":
                return new MAD(conf);
            case "MCD":
                return new MinCovDet(conf);
            case "TKDE":
                return new TKDE(conf);
            default:
                throw new ConfigurationException("Bad Transform Type");
        }
    }
}

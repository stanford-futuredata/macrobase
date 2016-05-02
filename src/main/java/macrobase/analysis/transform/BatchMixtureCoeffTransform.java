package macrobase.analysis.transform;

import macrobase.analysis.stats.mixture.BatchMixtureModel;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.List;

/**
 * Similar to BatchScoreFeatureTransform, but instead of using the
 * score, operates on mixture models and returns probabilities of
 * data belonging to different clusters.
 */
public class BatchMixtureCoeffTransform extends BatchScoreFeatureTransform {
    protected BatchMixtureModel mixtureModel;

    public BatchMixtureCoeffTransform(MacroBaseConf conf, MacroBaseConf.TransformType transformType) throws ConfigurationException {
        super(conf, transformType);
        this.mixtureModel = (BatchMixtureModel) this.batchTrainScore;
    }

    @Override
    public void consume(List<Datum> records) {
        mixtureModel.train(records);
        for (Datum d : records) {
            output.add(new Datum(d, mixtureModel.getClusterProbabilities(d)));
        }
    }

    public BatchMixtureModel getMixtureModel() {
        return mixtureModel;
    }
}

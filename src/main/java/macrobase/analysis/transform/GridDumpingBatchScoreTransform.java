package macrobase.analysis.transform;

import macrobase.analysis.pipeline.operator.MBStream;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.mixture.BatchMixtureModel;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.JsonUtils;
import macrobase.diagnostics.ScoreDumper;
import macrobase.util.AlgebraUtils;

import java.util.List;

public class GridDumpingBatchScoreTransform implements FeatureTransform {

    private final String dumpFilename;
    private final Integer dimensionsPerGrid;
    private final BatchScoreFeatureTransform underlyingTransform;
    private final String dumpMixtureComponents;

    public GridDumpingBatchScoreTransform(MacroBaseConf conf, BatchScoreFeatureTransform batchScoreFeatureTransform) {
        this.dumpFilename = conf.getString(MacroBaseConf.DUMP_SCORE_GRID, MacroBaseDefaults.DUMP_SCORE_GRID);
        this.dimensionsPerGrid = conf.getInt(MacroBaseConf.NUM_SCORE_GRID_POINTS_PER_DIMENSION, MacroBaseDefaults.NUM_SCORE_GRID_POINTS_PER_DIMENSION);
        this.dumpMixtureComponents = conf.getString(MacroBaseConf.DUMP_MIXTURE_COMPONENTS, MacroBaseDefaults.DUMP_MIXTURE_COMPONENTS);
        this.underlyingTransform = batchScoreFeatureTransform;
    }
    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        underlyingTransform.consume(records);
        if (dumpFilename != null) {
            BatchTrainScore batchTrainScore = underlyingTransform.getBatchTrainScore();
            ScoreDumper.tryToDumpScoredGrid(batchTrainScore, AlgebraUtils.getBoundingBox(records), dimensionsPerGrid, dumpFilename);
        }

        if (this.dumpMixtureComponents != null) {
            BatchMixtureModel mixtureModel = (BatchMixtureModel) underlyingTransform.getBatchTrainScore();
            JsonUtils.tryToDumpAsJson(mixtureModel.getClusterWeights(), "weights-" + dumpMixtureComponents);
            JsonUtils.tryToDumpAsJson(mixtureModel.getClusterCovariances(), "covariances-" + dumpMixtureComponents);
            JsonUtils.tryToDumpAsJson(mixtureModel.getClusterCenters(), "centers-" + dumpMixtureComponents);
        }

    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return underlyingTransform.getStream();
    }
}

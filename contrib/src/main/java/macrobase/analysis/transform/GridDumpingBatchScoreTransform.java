package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.mixture.BatchMixtureModel;
import macrobase.analysis.stats.mixture.GMMConf;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.JsonUtils;
import macrobase.diagnostics.ScoreDumper;
import macrobase.util.AlgebraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GridDumpingBatchScoreTransform extends FeatureTransform {
    public static final String DUMP_SCORE_GRID = "macrobase.diagnostic.dumpScoreGrid";
    public static final String NUM_SCORE_GRID_POINTS_PER_DIMENSION = "macrobase.diagnostic.gridPointsPerDimension";
    public static final String DUMP_MIXTURE_COMPONENTS = "macrobase.diagnostic.dumpMixtureComponents";

    public static final Integer NUM_SCORE_GRID_POINTS_PER_DIMENSION_DEFAULT = 1000;
    public static final String DUMP_SCORE_GRID_DEFAULT = null;

    private static final Logger log = LoggerFactory.getLogger(GridDumpingBatchScoreTransform.class);

    private final String dumpFilename;
    private final Integer dimensionsPerGrid;
    private final BatchScoreFeatureTransform underlyingTransform;
    private final String dumpMixtureComponents;

    public GridDumpingBatchScoreTransform(MacroBaseConf conf, BatchScoreFeatureTransform batchScoreFeatureTransform) {
        this.dumpFilename = conf.getString(DUMP_SCORE_GRID, DUMP_SCORE_GRID_DEFAULT);
        this.dimensionsPerGrid = conf.getInt(NUM_SCORE_GRID_POINTS_PER_DIMENSION, NUM_SCORE_GRID_POINTS_PER_DIMENSION_DEFAULT);
        this.dumpMixtureComponents = conf.getString(DUMP_MIXTURE_COMPONENTS, GMMConf.DUMP_MIXTURE_COMPONENTS_DEFAULT);
        this.underlyingTransform = batchScoreFeatureTransform;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        underlyingTransform.consume(records);
        log.debug("dumping");
        if (dumpFilename != null) {
            BatchTrainScore batchTrainScore = underlyingTransform.getBatchTrainScore();
            ScoreDumper.tryToDumpScoredGrid(batchTrainScore, AlgebraUtils.getBoundingBox(records), dimensionsPerGrid, dumpFilename);
        }

        if (this.dumpMixtureComponents != null) {
            BatchMixtureModel mixtureModel = (BatchMixtureModel) underlyingTransform.getBatchTrainScore();
            JsonUtils.tryToDumpAsJson(mixtureModel.getClusterProportions(), "weights-" + dumpMixtureComponents);
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

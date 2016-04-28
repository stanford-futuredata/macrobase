package macrobase.analysis.transform;

import macrobase.analysis.pipeline.operator.MBStream;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.ScoreDumper;
import macrobase.util.AlgebraUtils;

import java.util.List;

public class GridDumpingBatchScoreTransform implements FeatureTransform {

    private final String dumpFilename;
    private final Integer dimensionsPerGrid;
    private final BatchScoreFeatureTransform underlyingTransform;

    public GridDumpingBatchScoreTransform(MacroBaseConf conf, BatchScoreFeatureTransform batchScoreFeatureTransform) {
        this.dumpFilename = conf.getString(MacroBaseConf.DUMP_SCORE_GRID, MacroBaseDefaults.DUMP_SCORE_GRID);
        this.dimensionsPerGrid = conf.getInt(MacroBaseConf.NUM_SCORE_GRID_POINTS_PER_DIMENSION, MacroBaseDefaults.NUM_SCORE_GRID_POINTS_PER_DIMENSION);
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

    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return underlyingTransform.getStream();
    }
}

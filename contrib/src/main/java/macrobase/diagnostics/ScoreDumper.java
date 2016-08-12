package macrobase.diagnostics;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.DiagnosticsUtils;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ScoreDumper {
    public static final String SCORED_DATA_FILE = "macrobase.diagnostic.scoreDataFile";
    private final MacroBaseConf conf;
    private final String scoreFile;

    public ScoreDumper(MacroBaseConf conf) {
        this.conf = conf;
        this.scoreFile = conf.getString(SCORED_DATA_FILE, "");
    }

    public void dumpScores(BatchTrainScore batchTrainScore, List<Datum> data) {
        List<MetricsAndScore> metricsAndScores = new ArrayList<>(data.size());
        for (Datum d : data) {
            double score = batchTrainScore.score(d);
            metricsAndScores.add(new MetricsAndScore(d.metrics(), score));
        }
        try {
            JsonUtils.dumpAsJson(metricsAndScores, scoreFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void dumpScores(BatchTrainScore batchTrainScore, double[][] boundaries, double delta) {
        List<Datum> gridData = DiagnosticsUtils.createGridFixedIncrement(boundaries, delta);
        dumpScores(batchTrainScore, gridData);
    }

    public static void tryToDumpScoredGrid(BatchTrainScore batchTrainScore, double[][] boundingBox, int pointsPerDimension, String filename) {
        List<Datum> gridData = DiagnosticsUtils.createGridFixedSize(boundingBox, pointsPerDimension);

        List<MetricsAndScore> scoredGrid = gridData.stream()
                .map(d -> new MetricsAndScore(d.metrics(), batchTrainScore.score(d)))
                .collect(Collectors.toList());
        JsonUtils.tryToDumpAsJson(scoredGrid, filename);
    }
}

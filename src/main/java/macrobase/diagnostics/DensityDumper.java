package macrobase.diagnostics;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.DensityEstimater;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.DiagnosticsUtils;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DensityDumper {
    private final MacroBaseConf conf;
    private final String scoreFile;

    public DensityDumper(MacroBaseConf conf) {
        this.conf = conf;
        this.scoreFile = conf.getString(MacroBaseConf.SCORED_DATA_FILE, "");
    }

    public void dumpScores(BatchTrainScore batchTrainScore, List<Datum> data) {
        List<MetricsAndDensity> metricsAndDensities = new ArrayList<>(data.size());
        for (Datum d : data) {
            double score = batchTrainScore.score(d);
            metricsAndDensities.add(new MetricsAndDensity(d.getMetrics(), score));
        }
        try {
            JsonUtils.dumpAsJson(metricsAndDensities, scoreFile);
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

    public static void tryToDumpScoredGrid(DensityEstimater densityEstimater, double[][] boundingBox, int pointsPerDimension, String filename) {
        List<Datum> gridData = DiagnosticsUtils.createGridFixedSize(boundingBox, pointsPerDimension);

        List<MetricsAndDensity> scoredGrid = gridData.stream()
                .map(d -> new MetricsAndDensity(d.getMetrics(), densityEstimater.density(d)))
                .collect(Collectors.toList());
        JsonUtils.tryToDumpAsJson(scoredGrid, filename);
    }
}

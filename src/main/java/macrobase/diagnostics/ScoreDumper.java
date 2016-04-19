package macrobase.diagnostics;

import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.DiagnosticsUtils;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class ScoreDumper {
    private final MacroBaseConf conf;
    private final String scoreFile;

    public ScoreDumper(MacroBaseConf conf) {
        this.conf = conf;
        this.scoreFile = conf.getString(MacroBaseConf.SCORE_DUMP_FILE_CONFIG_PARAM, "");
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
        List<Datum> gridData = DiagnosticsUtils.create2DGrid(boundaries, delta);
        dumpScores(batchTrainScore, gridData);
    }
}

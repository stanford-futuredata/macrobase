package macrobase.diagnostic.tasks;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.analysis.BaseAnalyzer;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ScoreDumpDiagnostic extends ConfiguredCommand<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(ScoreDumpDiagnostic.class);

    public static final String SCORE_DUMP_FILE_CONFIG_PARAM = "macrobase.diagnostic.dumpScoreFile";

    public ScoreDumpDiagnostic() {
        super("dump", "Dump scores to file.");
    }

    @Override
    protected void run(Bootstrap<MacroBaseConf> bootstrap, Namespace namespace, MacroBaseConf configuration) throws Exception {
        DiagnosticTask task = new DiagnosticTask(configuration);
        task.run();
    }

    private class DiagnosticTask extends BaseAnalyzer {
        public DiagnosticTask(MacroBaseConf conf) throws ConfigurationException {
            super(conf);
        }

        public void run() throws ConfigurationException, IOException, SQLException {
            assert (conf.isSet(SCORE_DUMP_FILE_CONFIG_PARAM));

            DatumEncoder encoder = new DatumEncoder();

            // OUTLIER ANALYSIS
            List<Datum> data = constructLoader().getData(encoder);
            OutlierDetector detector = constructDetector(randomSeed);

            OutlierDetector.BatchResult or;
            if (forceUsePercentile || (!forceUseZScore && targetPercentile > 0)) {
                or = detector.classifyBatchByPercentile(data, targetPercentile);
            } else {
                or = detector.classifyBatchByZScoreEquivalent(data, zScore);
            }

            String scoreFile = conf.getString(SCORE_DUMP_FILE_CONFIG_PARAM);

            Gson gson = new GsonBuilder()
                    .enableComplexMapKeySerialization()
                    .serializeNulls()
                    .setPrettyPrinting()
                    .setVersion(1.0)
                    .create();
            final File dir = new File("target/scores");
            dir.mkdirs();
            try (PrintStream out = new PrintStream(new File(dir, scoreFile),
                                                   "UTF-8")) {
                out.println(gson.toJson(or));
            }

            try (PrintStream out = new PrintStream(
                    new File(dir, "outliers_" + scoreFile), "UTF-8")) {
                out.println(gson.toJson(or.getOutliers()));
            }
            try (PrintStream out = new PrintStream(
                    new File(dir, "inliers_" + scoreFile), "UTF-8")) {
                out.println(gson.toJson(or.getInliers()));
            }
        }
    }
}

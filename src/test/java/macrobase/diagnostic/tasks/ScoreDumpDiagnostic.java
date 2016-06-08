package macrobase.diagnostic.tasks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.ScoreDumper;
import macrobase.ingest.DatumEncoder;
import macrobase.util.Drainer;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

public class ScoreDumpDiagnostic extends ConfiguredCommand<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(ScoreDumpDiagnostic.class);

    public ScoreDumpDiagnostic() {
        super("dump", "Dump scores to file.");
    }

    @Override
    protected void run(Bootstrap<MacroBaseConf> bootstrap, Namespace namespace, MacroBaseConf configuration) throws Exception {
        DiagnosticTask task = new DiagnosticTask();
        task.initialize(configuration);
        task.run();
    }

    private class DiagnosticTask extends BasePipeline {

        public List<AnalysisResult> run() throws Exception {
            List<Datum> data = Drainer.drainIngest(conf);
            BatchTrainScore detector = conf.constructTransform(conf.getTransformType());
            
            ScoreDumper dumper = new ScoreDumper(conf);
            dumper.dumpScores(detector, data);
            
            // Required to shutdown JRI if it is running
            System.exit(0);

            // Need to appease Java compiler
            return null;
        }
    }
}

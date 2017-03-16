package macrobase.runtime.command;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.MacroBase;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MacroBasePipelineCommand extends ConfiguredCommand<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(MacroBasePipelineCommand.class);

    public MacroBasePipelineCommand() {
        super("pipeline", "Run pipeline without starting server.");
    }

    @Override
    protected void run(Bootstrap<MacroBaseConf> bootstrap,
                       Namespace namespace,
                       MacroBaseConf configuration) throws Exception {
        configuration.loadSystemProperties();
        Class c = Class.forName(configuration.getString(MacroBaseConf.PIPELINE_NAME));
        Object ao = c.newInstance();

        if (!(ao instanceof Pipeline)) {
            log.error("{} is not an instance of Pipeline! Exiting...");
            return;
        }

        List<AnalysisResult> results = ((Pipeline) ao).initialize(configuration).run();

        MacroBase.reporter.report();

        log.info("Result: {}", results);
    }
}

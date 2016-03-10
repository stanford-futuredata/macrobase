package macrobase.runtime.standalone;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import macrobase.MacroBase;
import macrobase.analysis.pipeline.BasicOneShotEWStreamingPipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseStreamingCommand extends ConfiguredCommand<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseStreamingCommand.class);

    public MacroBaseStreamingCommand() {
        super("streaming", "Run streaming task without starting server.");
    }

    @Override
    protected void run(Bootstrap<MacroBaseConf> bootstrap,
                       Namespace namespace,
                       MacroBaseConf configuration) throws Exception {
        configuration.loadSystemProperties();
        BasicOneShotEWStreamingPipeline analyzer = new BasicOneShotEWStreamingPipeline(configuration);

        AnalysisResult result = analyzer.next();
        MacroBase.reporter.report();

        log.info("Result: {}", result);
    }
}

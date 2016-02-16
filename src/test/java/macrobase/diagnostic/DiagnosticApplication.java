package macrobase.diagnostic;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.conf.MacroBaseConf;
import macrobase.diagnostic.tasks.ScoreDumpDiagnostic;

public class DiagnosticApplication extends Application<MacroBaseConf> {
    @Override
    public void initialize(Bootstrap<MacroBaseConf> bootstrap) {
        bootstrap.addCommand(new ScoreDumpDiagnostic());
    }

    @Override
    public void run(MacroBaseConf configuration,
                    Environment environment) throws Exception {
    }
}

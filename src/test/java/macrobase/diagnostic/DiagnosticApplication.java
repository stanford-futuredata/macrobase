package macrobase.diagnostic;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.conf.MacroBaseConf;
import macrobase.diagnostic.tasks.HeldOutDataLogLikelihoodCalc;
import macrobase.diagnostic.tasks.ScoreDumpDiagnostic;
import macrobase.diagnostic.tasks.TrueDesnityMISECalculator;

public class DiagnosticApplication extends Application<MacroBaseConf> {
    @Override
    public void initialize(Bootstrap<MacroBaseConf> bootstrap) {
        bootstrap.addCommand(new TrueDesnityMISECalculator());
        bootstrap.addCommand(new ScoreDumpDiagnostic());
        bootstrap.addCommand(new HeldOutDataLogLikelihoodCalc());
    }

    @Override
    public void run(MacroBaseConf configuration,
                    Environment environment) throws Exception {
    }
}

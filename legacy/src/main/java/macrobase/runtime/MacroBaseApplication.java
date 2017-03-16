package macrobase.runtime;

import com.codahale.metrics.health.HealthCheck;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.conf.MacroBaseConf;
import macrobase.runtime.command.MacroBasePipelineCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseApplication extends Application<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseApplication.class);

    public static void main(String[] args) throws Exception {
        new MacroBaseApplication().run(args);
    }

    @Override
    public String getName() {
        return "macrobase";
    }

    @Override
    public void initialize(Bootstrap<MacroBaseConf> bootstrap) {
        bootstrap.addCommand(new MacroBasePipelineCommand());
    }

    @Override
    public void run(MacroBaseConf configuration,
                    Environment environment) throws Exception {
        configuration.loadSystemProperties();
        environment.healthChecks().register("basic", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return null;
            }
        });
    }
}

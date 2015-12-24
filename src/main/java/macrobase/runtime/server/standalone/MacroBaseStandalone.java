package macrobase.runtime.server.standalone;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Created by pbailis on 12/24/15.
 */
public class MacroBaseStandalone extends ConfiguredCommand<StandaloneConfiguration> {
    public MacroBaseStandalone() {
        super("standalone", "Run task without starting server.");
    }

    @Override
    protected void run(Bootstrap<StandaloneConfiguration> bootstrap,
                       Namespace namespace,
                       StandaloneConfiguration configuration) throws Exception {
        System.out.println("CONFIGURATION IS "+configuration.getTaskName());
    }
}

package macrobase.runtime;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.analysis.BaseAnalyzer;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.DataLoader;
import macrobase.ingest.PostgresLoader;
import macrobase.ingest.SQLLoader;
import macrobase.runtime.healthcheck.TemplateHealthCheck;
import macrobase.runtime.resources.AnalyzeResource;
import macrobase.runtime.resources.RowSetResource;
import macrobase.runtime.resources.SchemaResource;
import macrobase.runtime.standalone.MacroBaseBatchCommand;
import macrobase.runtime.standalone.MacroBaseStreamingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseServer extends Application<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseServer.class);

    public static void main(String[] args) throws Exception {
        new MacroBaseServer().run(args);
    }

    @Override
    public String getName() {
        return "macrobase";
    }

    @Override
    public void initialize(Bootstrap<MacroBaseConf> bootstrap) {
        bootstrap.addCommand(new MacroBaseBatchCommand());
        bootstrap.addCommand(new MacroBaseStreamingCommand());
        bootstrap.addBundle(new AssetsBundle("/frontend", "/", "console.html"));
    }

    @Override
    public void run(MacroBaseConf configuration,
                    Environment environment) throws Exception {
        environment.jersey().register(new SchemaResource(configuration));
        environment.jersey().register(new RowSetResource(configuration));
        environment.jersey().register(new AnalyzeResource(configuration));

        final TemplateHealthCheck healthCheck =
                new TemplateHealthCheck("macrobase");
        environment.healthChecks().register("template", healthCheck);

        environment.jersey().setUrlPattern("/api/*");
    }
}
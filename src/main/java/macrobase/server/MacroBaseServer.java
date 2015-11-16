package macrobase.server;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.ingest.PostgresLoader;
import macrobase.server.healthcheck.TemplateHealthCheck;
import macrobase.server.resources.AnalyzeResource;
import macrobase.server.resources.HelloResource;
import macrobase.server.resources.RowSetResource;
import macrobase.server.resources.SchemaResource;

public class MacroBaseServer extends Application<MacroBaseConfiguration> {
    public static void main(String[] args) throws Exception {
        new MacroBaseServer().run(args);
    }

    @Override
    public String getName() {
        return "hello-world";
    }

    @Override
    public void initialize(Bootstrap<MacroBaseConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/frontend", "/", "console.html"));
    }

    @Override
    public void run(MacroBaseConfiguration configuration,
                    Environment environment) throws Exception {

        PostgresLoader loader = new PostgresLoader();

        final HelloResource resource = new HelloResource(
                configuration.getTemplate(),
                configuration.getDefaultName()
        );
        environment.jersey().register(resource);
        environment.jersey().register(new SchemaResource(loader));
        environment.jersey().register(new RowSetResource(loader));
        environment.jersey().register(new AnalyzeResource(loader));

        final TemplateHealthCheck healthCheck =
                new TemplateHealthCheck(configuration.getTemplate());
        environment.healthChecks().register("template", healthCheck);
        environment.jersey().register(resource);

        environment.jersey().setUrlPattern("/api/*");
    }

}
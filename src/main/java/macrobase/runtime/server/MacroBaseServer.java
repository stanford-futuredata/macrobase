package macrobase.runtime.server;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.ingest.PostgresLoader;
import macrobase.ingest.SQLLoader;
import macrobase.runtime.server.healthcheck.TemplateHealthCheck;
import macrobase.runtime.server.resources.AnalyzeResource;
import macrobase.runtime.server.resources.HelloResource;
import macrobase.runtime.server.resources.RowSetResource;
import macrobase.runtime.server.resources.SchemaResource;
import macrobase.runtime.server.standalone.MacroBaseStandalone;

public class MacroBaseServer extends Application<ServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new MacroBaseServer().run(args);
    }

    @Override
    public String getName() {
        return "hello-world";
    }

    @Override
    public void initialize(Bootstrap<ServerConfiguration> bootstrap) {
        bootstrap.addCommand(new MacroBaseStandalone());
        bootstrap.addBundle(new AssetsBundle("/frontend", "/", "console.html"));
    }

    @Override
    public void run(ServerConfiguration configuration,
                    Environment environment) throws Exception {

        SQLLoader loader = new PostgresLoader();

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
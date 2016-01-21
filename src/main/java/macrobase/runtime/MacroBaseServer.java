package macrobase.runtime;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.ingest.MemoryCachingPostgresLoader;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.SQLLoader;
import macrobase.runtime.healthcheck.TemplateHealthCheck;
import macrobase.runtime.resources.AnalyzeResource;
import macrobase.runtime.resources.RowSetResource;
import macrobase.runtime.resources.SchemaResource;
import macrobase.runtime.standalone.batch.MacroBaseBatchCommand;
import macrobase.runtime.standalone.streaming.MacroBaseStreamingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class MacroBaseServer extends Application<ServerConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseServer.class);


    public static void main(String[] args) throws Exception {
        new MacroBaseServer().run(args);
    }

    @Override
    public String getName() {
        return "macrobase";
    }

    @Override
    public void initialize(Bootstrap<ServerConfiguration> bootstrap) {
        bootstrap.addCommand(new MacroBaseBatchCommand());
        bootstrap.addCommand(new MacroBaseStreamingCommand());
        bootstrap.addBundle(new AssetsBundle("/frontend", "/", "console.html"));
    }

    @Override
    public void run(ServerConfiguration configuration,
                    Environment environment) throws Exception {

        SQLLoader loader = new MemoryCachingPostgresLoader();

        if(configuration.doPreLoadData() && (!configuration.getPreLoad().isEmpty() || true)) {
            for(ServerConfiguration.PreLoadData pld : configuration.getPreLoad()) {
                loader.setDatabaseCredentials(pld.getDbUser(), pld.getDbPassword());
                try {
                    log.info("Pre-loading {} from {}...", pld.getBaseQuery(), pld.getDbUrl());
                    loader.connect(pld.getDbUrl());
                    loader.getData(new DatumEncoder(),
                                   pld.getTargetAttributes(),
                                   pld.getTargetLowMetrics(),
                                   pld.getTargetHighMetrics(),
                                   pld.getBaseQuery());
                    log.info("...loaded!");
                } catch (SQLException e) {
                    log.error("SQL exception while loading data: {} {}", pld.getBaseQuery(), e);
                }

            }

        }

        environment.jersey().register(new SchemaResource(loader));
        environment.jersey().register(new RowSetResource(loader));
        environment.jersey().register(new AnalyzeResource(loader));

        final TemplateHealthCheck healthCheck =
                new TemplateHealthCheck("macrobase");
        environment.healthChecks().register("template", healthCheck);

        environment.jersey().setUrlPattern("/api/*");
    }

}
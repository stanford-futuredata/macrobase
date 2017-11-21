package edu.stanford.futuredata.macrobase.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.applications.telemetry.LogManager;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.pipeline.PipelineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import static spark.Spark.exception;
import static spark.Spark.post;

public class RestServer {
    private static Logger log = LoggerFactory.getLogger(RestServer.class);

    public static void main(String[] args) throws Exception {

        final String configFile = args[0];
        PipelineConfig conf = PipelineConfig.fromYamlFile(configFile);
        // Use the Java Services SDK test token from https://aria.microsoft.com/ as default if no
        // SDK token is provided
        final String sdkToken = conf.get("sdkToken",
                "48fdfa728c9b4f999b4414ebe8bd5012-d052eb80-e696-4f88-a984-b0523b6397de-7757");

        LogManager.initialize(sdkToken);

        post("/query", RestServer::processBasicBatchQuery, RestServer::toJsonString);

        exception(Exception.class, (exception, request, response) -> {
            log.error("An exception occurred: ", exception);
        });

        // Flush the queue of records in memory and free up the Aria resources.
        // This call blocks until complete.
        LogManager.flushAndTearDown();
    }

    public static Explanation processBasicBatchQuery(
            Request req, Response res
    ) throws Exception {
        res.type("application/json");
        PipelineConfig conf = PipelineConfig.fromJsonString(req.body());
        Pipeline p = PipelineUtils.createPipeline(conf);
        Explanation e = p.results();
        return e;
    }

    public static String toJsonString(Object o) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(o);
    }
}

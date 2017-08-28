package edu.stanford.futuredata.macrobase.contrib.aria;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.pipeline.BasicBatchPipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import static spark.Spark.exception;
import static spark.Spark.post;

public class AriaRestServer {
    private static Logger log = LoggerFactory.getLogger(AriaRestServer.class);

    public static void main(String[] args) {
        post("/query", AriaRestServer::processBasicBatchQuery, AriaRestServer::toJsonString);

        exception(Exception.class, (exception, request, response) -> {
            log.error("An exception occurred: ", exception);
        });
    }

    public static Explanation processBasicBatchQuery(
            Request req, Response res
    ) throws Exception {
        res.type("application/json");
        PipelineConfig conf = PipelineConfig.fromJsonString(req.body());
        Explanation e = new AriaCubePipeline(conf).results();
        return e;
    }

    public static String toJsonString(Object o) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(o);
    }
}

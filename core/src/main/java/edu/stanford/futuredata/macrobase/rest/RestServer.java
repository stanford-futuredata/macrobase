package edu.stanford.futuredata.macrobase.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.pipeline.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

public class RestServer {
    private static Logger log = LoggerFactory.getLogger(RestServer.class);

    public static void main(String[] args) {
        post("/query", RestServer::processBasicBatchQuery, RestServer::toJsonString);

        exception(Exception.class, (exception, request, response) -> {
            log.error("An exception occurred: ", exception);
        });
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

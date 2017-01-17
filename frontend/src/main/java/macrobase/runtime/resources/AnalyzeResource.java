package macrobase.runtime.resources;

import macrobase.MacroBase;
import macrobase.analysis.pipeline.BasicBatchedPipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Path("/analyze")
@Produces(MediaType.APPLICATION_JSON)
public class AnalyzeResource extends BaseResource {
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    static class AnalysisRequest {
        public String pgUrl;
        public String baseQuery;
        public List<String> attributes;
        public List<String> highMetrics;
        public List<String> lowMetrics;
    }

    static class AnalysisResponse {
        public List<AnalysisResult> results;
        public String errorMessage;
    }

    public AnalyzeResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public AnalysisResponse getAnalysis(AnalysisRequest request) {
        AnalysisResponse response = new AnalysisResponse();

        try {
            List<String> allMetrics = new ArrayList<>();
            allMetrics.addAll(request.highMetrics);
            allMetrics.addAll(request.lowMetrics);

            conf.set(MacroBaseConf.DB_URL, request.pgUrl);
            conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);
            conf.set(MacroBaseConf.ATTRIBUTES, request.attributes);
            conf.set(MacroBaseConf.METRICS, allMetrics);
            conf.set(MacroBaseConf.LOW_METRIC_TRANSFORM, request.lowMetrics);
            conf.set(MacroBaseConf.USE_PERCENTILE, true);

            // temp hack to enable CSV loading
            if (request.baseQuery.contains("csv://")) {
                conf.set(MacroBaseConf.CSV_INPUT_FILE, request.baseQuery.replace("csv://", ""));
                conf.set(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseConf.DataIngesterType.CSV_LOADER);
            }

            Class c = Class.forName(conf.getString(MacroBaseConf.PIPELINE_NAME, BasicBatchedPipeline.class.getName()));
            Object ao = c.newInstance();

            if (!(ao instanceof Pipeline)) {
                log.error("{} is not an instance of Pipeline! Exiting...", ao);
                response.errorMessage = "Requested pipeline of type "+c.getName()+ " is not a Pipeline";
                return response;
            }
            Pipeline pipeline = (Pipeline) ao;

            List<AnalysisResult> results = pipeline.initialize(conf).run();

            for (AnalysisResult result : results) {
                if (result.getItemSets().size() > 1000) {
                    log.warn("Very large result set! {}; truncating to 1000", result.getItemSets().size());
                    result.setItemSets(result.getItemSets().subList(0, 1000));
                }
            }

            response.results = results;

            MacroBase.reporter.report();
        } catch (Exception e) {
            log.error("An error occurred while processing a request: {}", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }

        return response;
    }
}

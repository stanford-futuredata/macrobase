package macrobase.runtime.resources;

import macrobase.MacroBase;
import macrobase.analysis.pipeline.WeirdTransitionPipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
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

    public AnalyzeResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public List<AnalysisResult> getAnalysis(AnalysisRequest request) throws Exception {
        conf.set(MacroBaseConf.DB_URL, request.pgUrl);
        conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);
        conf.set(MacroBaseConf.ATTRIBUTES, request.attributes);
        conf.set(MacroBaseConf.HIGH_METRICS, request.highMetrics);
        conf.set(MacroBaseConf.LOW_METRICS, request.lowMetrics);
        conf.set(MacroBaseConf.USE_PERCENTILE, true);

        List<AnalysisResult> results = new WeirdTransitionPipeline().initialize(conf).run();

        for(AnalysisResult result: results) {
            if (result.getItemSets().size() > 1000) {
                log.warn("Very large result set! {}; truncating to 1000", result.getItemSets().size());
                result.setItemSets(result.getItemSets().subList(0, 1000));
            }
        }
        
        MacroBase.reporter.report();
        return results;
    }
}

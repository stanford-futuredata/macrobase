package macrobase.runtime.server.resources;

import macrobase.MacroBase;
import macrobase.analysis.CoreAnalyzer;
import macrobase.ingest.PostgresLoader;
import macrobase.analysis.result.AnalysisResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/analyze")
@Produces(MediaType.APPLICATION_JSON)
public class AnalyzeResource {
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    static class AnalysisRequest {
        public String pgUrl;
        public String baseQuery;
        public List<String> attributes;
        public List<String> highMetrics;
        public List<String> lowMetrics;
    }

    private PostgresLoader loader;

    public AnalyzeResource(PostgresLoader _loader) {
        loader = _loader;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public AnalysisResult getAnalysis(AnalysisRequest request) throws Exception {
        loader.connect(request.pgUrl);
        AnalysisResult result = CoreAnalyzer.analyze(loader,
                                                     request.attributes,
                                                     request.lowMetrics,
                                                     request.highMetrics,
                                                     request.baseQuery);
        if(result.getItemSets().size() > 1000) {
            log.warn("Very large result set! {}; truncating to 1000", result.getItemSets().size());
            result.setItemSets(result.getItemSets().subList(0, 1000));
        }

        MacroBase.reporter.report();
        return result;
    }
}

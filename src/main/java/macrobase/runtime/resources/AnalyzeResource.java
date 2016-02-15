package macrobase.runtime.resources;

import macrobase.MacroBase;
import macrobase.analysis.BatchAnalyzer;
import macrobase.ingest.SQLLoader;
import macrobase.analysis.result.AnalysisResult;
import macrobase.ingest.transform.ZeroToOneLinearTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
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

    private SQLLoader loader;

    public AnalyzeResource(SQLLoader _loader) {
        loader = _loader;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public AnalysisResult getAnalysis(AnalysisRequest request) throws Exception {
        loader.connect(request.pgUrl);
        BatchAnalyzer analyzer = new BatchAnalyzer();
        AnalysisResult result = analyzer.analyze(loader,
                                                 request.attributes,
                                                 request.lowMetrics,
                                                 request.highMetrics,
                                                 new ArrayList<>(),
                                                 request.baseQuery,
                                                 new ZeroToOneLinearTransformation());
        if(result.getItemSets().size() > 1000) {
            log.warn("Very large result set! {}; truncating to 1000", result.getItemSets().size());
            result.setItemSets(result.getItemSets().subList(0, 1000));
        }

        MacroBase.reporter.report();
        return result;
    }
}

package edu.stanford.futuredata.macrobase.contrib.aria;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Aria pipeline for cubed data: load, classify, and then explain
 */
public class AriaCubePipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger("AriaCubePipeline");

    // API ingester params
    private String inputURI;
    private String startTimeStamp;
    private String endTimeStamp;
    private Map<String, String> restHeader;

    private String metric;
    private String classifierType;
    private double cutoff;
    private boolean includeHi;
    private boolean includeLo;

    private List<String> attributes;
    private double minSupport;
    private double minRatioMetric;

    public AriaCubePipeline(PipelineConfig conf) {
        inputURI = conf.get("inputURI");
        startTimeStamp = conf.get("startTime");
        endTimeStamp = conf.get("endTime");
        restHeader = conf.get("restHeader");

        metric = conf.get("metric");
        classifierType = conf.get("classifier");
        cutoff = conf.get("cutoff", 1.0);
        includeHi = conf.get("includeHi", true);
        includeLo = conf.get("includeLo", false);

        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 0.01);
        minRatioMetric = conf.get("minRatioMetric", 3.0);
    }

    public Explanation results() throws Exception {
        long startTime = System.currentTimeMillis();
        CubeQueryService cs = new CubeQueryService(
                inputURI,
                restHeader,
                startTimeStamp,
                endTimeStamp
        );
        DataFrame df = cs.getFrequentCubeEntries(
                metric,
                attributes,
                CubeQueryService.quantileOperators,
                minSupport
        );
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loading time: {}", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Attributes: {}", attributes);

        System.out.println(df);
        return null;
    }

}

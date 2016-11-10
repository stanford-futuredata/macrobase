package macrobase.runtime.resources;

import com.google.common.collect.Lists;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.stats.MAD;
import macrobase.analysis.stats.MinCovDet;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LowMetricTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.ingest.result.RowSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 Returns a CSV of points matching this outlier
 */
@Path("/analyze/target")
@Produces(MediaType.APPLICATION_JSON)
public class TargetedOutlierResource extends BaseResource {
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    public static class TargetedOutlierRequest {
        public String pgUrl;
        public String baseQuery;
        public List<RowRequestPair> columnValues;
        public List<String> attributes;
        public List<String> highMetrics;
        public List<String> lowMetrics;

        public static class RowRequestPair {
            public String column;
            public String value;
        }
    }

    public static class FormattedRowSetResponse {
        public String response;
        public String errorMessage;
    }

    public TargetedOutlierResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public FormattedRowSetResponse getOutlierRows(TargetedOutlierRequest request) {
        FormattedRowSetResponse response = new FormattedRowSetResponse();

        try {
            conf.set(MacroBaseConf.DB_URL, request.pgUrl);
            conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);

            HashMap<String, String> preds = new HashMap<>();
            request.columnValues.stream().forEach(a -> preds.put(a.column, a.value));

            List<String> allMetrics = new ArrayList<>();
            allMetrics.addAll(request.highMetrics);
            allMetrics.addAll(request.lowMetrics);

            // splice in allMetrics into the basequery -- total hack

            RowSet candidateOutliersInGroup = getLoader().getRows(request.baseQuery.replace("SELECT ",
                                                                                            "SELECT " + String.join(
                                                                                                    ", ",
                                                                                                    allMetrics) + ", "),
                                                                  preds,
                                                                  100000000,
                                                                  0);

            // train a classifier based on all the input data

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

            DataIngester ingester = conf.constructIngester();
            List<Datum> data = ingester.getStream().drain();

            if(conf.isSet(MacroBaseConf.LOW_METRIC_TRANSFORM)) {
                LowMetricTransform lmt = new LowMetricTransform(conf);
                lmt.consume(data);
                data = lmt.getStream().drain();
            }

            BatchScoreFeatureTransform btst = new BatchScoreFeatureTransform(conf);
            BatchTrainScore bts = btst.getBatchTrainScore();

            bts.train(data);
            double[] scores = data.stream().mapToDouble(d -> bts.score(d)).toArray();

            Percentile pCalc = new Percentile().withNaNStrategy(NaNStrategy.MAXIMAL);
            pCalc.setData(scores);
            double cutoff = pCalc.evaluate(scores, conf.getDouble(MacroBaseConf.TARGET_PERCENTILE, MacroBaseDefaults.TARGET_PERCENTILE) * 100);

            // now feed in the data from the subgroup to
            // determine which are outliers...

            Datum dummyDatum = new Datum();
            int metricsDimensions = allMetrics.size();
            List<RowSet.Row> actualOutliers = new ArrayList<>();
            for (RowSet.Row r : candidateOutliersInGroup.getRows()) {
                RealVector metricVector = new ArrayRealVector(metricsDimensions);
                for (int i = 0; i < metricsDimensions; ++i) {
                    Double val = Double.parseDouble(r.getColumnValues().get(i).getValue());

                    if (conf.isSet(MacroBaseConf.LOW_METRIC_TRANSFORM) &&
                        request.lowMetrics.contains(r.getColumnValues().get(i).getColumn())) {
                        val = Math.pow(Math.max(val, 0.1), -1);
                    }
                    metricVector.setEntry(i, val);
                }

                Datum toScore = new Datum(dummyDatum, metricVector);
                if(bts.score(toScore) >= cutoff) {
                    RowSet.Row truncated = new RowSet.Row(r.getColumnValues().subList(metricsDimensions,
                                                                                  r.getColumnValues().size()));
                    actualOutliers.add(truncated);
                }
            }

            log.info("Returning {} of {} points as outliers",
                     actualOutliers.size(),
                     candidateOutliersInGroup.getRows().size());

            StringWriter sw = new StringWriter();
            CSVPrinter printer = new CSVPrinter(sw, CSVFormat.DEFAULT);

            if(actualOutliers.size() == 0) {
                printer.printRecord(preds.keySet());
            } else {
                printer.printRecord(actualOutliers.get(0).getColumnValues().stream().map(a -> a.getColumn()).toArray());
                for (RowSet.Row row : actualOutliers) {
                    printer.printRecord(row.getColumnValues().stream().map(a -> a.getValue()).toArray());
                }
            }

            response.response = sw.toString();
        } catch (Exception e) {
            log.error("An error occurred while processing a request:", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }

        return response;
    }

}

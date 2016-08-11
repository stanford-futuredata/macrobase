package macrobase.ingest;

import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.runtime.resources.RowSetResource;

import java.util.ArrayList;
import java.util.List;

public abstract class DataIngester implements MBProducer<Datum> {
    protected final List<String> attributes;
    protected final List<String> auxiliaryAttributes;
    protected final List<String> contextualDiscreteAttributes;
    protected final List<String> contextualDoubleAttributes;
    protected final List<String> highMetrics;
    protected final List<String> lowMetrics;
    protected final MacroBaseConf conf;
    protected final Integer timeColumn;

    public DataIngester(MacroBaseConf conf) throws ConfigurationException {
        this.conf = conf;

        timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        lowMetrics = conf.getStringList(MacroBaseConf.LOW_METRICS);
        highMetrics = conf.getStringList(MacroBaseConf.HIGH_METRICS);
        auxiliaryAttributes = conf.getStringList(MacroBaseConf.AUXILIARY_ATTRIBUTES, new ArrayList<>());
        contextualDiscreteAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, MacroBaseDefaults.CONTEXTUAL_DISCRETE_ATTRIBUTES);
        contextualDoubleAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, MacroBaseDefaults.CONTEXTUAL_DOUBLE_ATTRIBUTES);
    }

    public abstract String getBaseQuery();

    public abstract Schema getSchema(String baseQuery) throws Exception;

    public abstract RowSet getRows(String baseQuery,
                                   List<RowSetResource.RowSetRequest.RowRequestPair> preds,
                                   int limit,
                                   int offset) throws Exception;
}

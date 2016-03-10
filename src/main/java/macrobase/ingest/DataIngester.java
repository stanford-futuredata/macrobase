package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class DataIngester implements Iterator<Datum> {
    protected final List<String> attributes;
    protected final List<String> auxiliaryAttributes;
    protected final List<String> contextualDiscreteAttributes;
    protected final List<String> contextualDoubleAttributes;
    protected final List<String> highMetrics;
    protected final List<String> lowMetrics;
    protected final MacroBaseConf conf;
    protected final String timeColumn;

    public DataIngester(MacroBaseConf conf) throws ConfigurationException {
        this.conf = conf;

        timeColumn = conf.getString(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        lowMetrics = conf.getStringList(MacroBaseConf.LOW_METRICS);
        highMetrics = conf.getStringList(MacroBaseConf.HIGH_METRICS);
        auxiliaryAttributes = conf.getStringList(MacroBaseConf.AUXILIARY_ATTRIBUTES, new ArrayList<>());
        contextualDiscreteAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, MacroBaseDefaults.CONTEXTUAL_DISCRETE_ATTRIBUTES);
        contextualDoubleAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, MacroBaseDefaults.CONTEXTUAL_DOUBLE_ATTRIBUTES);
    }


    public abstract String getBaseQuery();
}

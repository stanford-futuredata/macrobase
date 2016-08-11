package macrobase.ingest;

import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public abstract class DataIngester implements MBProducer<Datum> {
    protected final List<String> attributes;
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
    }
}

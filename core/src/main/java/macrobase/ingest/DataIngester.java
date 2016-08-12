package macrobase.ingest;

import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;

import java.util.List;

public abstract class DataIngester implements MBProducer<Datum> {
    protected final List<String> attributes;
    protected final List<String> metrics;
    protected final MacroBaseConf conf;
    protected final Integer timeColumn;

    public DataIngester(MacroBaseConf conf) throws ConfigurationException {
        this.conf = conf;

        timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        metrics = conf.getStringList(MacroBaseConf.METRICS);
    }
}

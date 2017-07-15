package macrobase.runtime.resources;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.DataIngester;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

abstract public class BaseResource {
    protected final MacroBaseConf conf;
    protected String configuredIngester;

    public BaseResource(MacroBaseConf conf) {

        this.conf = conf;
        configuredIngester = conf.getString(MacroBaseConf.DATA_LOADER_TYPE,
                                            MacroBaseDefaults.DATA_LOADER_TYPE.toString());
    }

    protected DataIngester getLoader() throws ConfigurationException, SQLException, IOException {
        // constructs ingester of type specified in conf file initially,
        // ^ used to be initially, now it's just whatever it currently is to work for CSV
        // or the default ingester
        // by default, REST calls may not have these defined.
        conf.set(MacroBaseConf.DATA_LOADER_TYPE, configuredIngester);
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, new ArrayList<>());
        return conf.constructIngester();
    }

}

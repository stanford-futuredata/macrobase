package macrobase.runtime.resources;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.SQLIngester;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

abstract public class BaseResource {
    protected final MacroBaseConf conf;

    public BaseResource(MacroBaseConf conf) {
        this.conf = conf;
    }

    protected SQLIngester getLoader() throws ConfigurationException, SQLException, IOException {
        // by default, REST calls may not have these defined.
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, new ArrayList<>());
        return (SQLIngester) conf.constructIngester();
    }
}

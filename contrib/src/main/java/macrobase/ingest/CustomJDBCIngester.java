package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

import java.sql.SQLException;

// allows creation of arbitrary JDBC ingesters (e.g., Oracle, SAP)
// as long as they're on the command line
public class CustomJDBCIngester extends SQLIngester {
    public static String CUSTOM_JDBC_DRIVER = "macrobase.loader.jdbc.driver";
    public static String CUSTOM_JDBC_PREFIX = "macrobase.loader.jdbc.urlprefix";

    private final String jdbcDriverString;
    private final String jdbcUrlPrefix;

    public CustomJDBCIngester(MacroBaseConf conf) throws ConfigurationException, SQLException {
        super(conf);
        jdbcDriverString = conf.getString(CUSTOM_JDBC_DRIVER);
        jdbcUrlPrefix = conf.getString(CUSTOM_JDBC_PREFIX);
    }

    @Override
    public String getDriverClass() {
        return jdbcDriverString;
    }

    @Override
    public String getJDBCUrlPrefix() {
        return jdbcUrlPrefix;
    }
}

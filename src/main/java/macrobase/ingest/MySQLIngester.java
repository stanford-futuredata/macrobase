package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

import java.sql.SQLException;

public class MySQLIngester extends SQLIngester {
    public MySQLIngester(MacroBaseConf conf) throws ConfigurationException, SQLException {
        super(conf);
    }

    @Override
    public String getDriverClass() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    public String getJDBCUrlPrefix() {
        return "jdbc:mysql:";
    }
}

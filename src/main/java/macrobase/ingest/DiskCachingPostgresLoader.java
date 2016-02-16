package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

import java.sql.SQLException;

public class DiskCachingPostgresLoader extends DiskCachingSQLLoader {
    public DiskCachingPostgresLoader(MacroBaseConf conf) throws ConfigurationException,
                                                                SQLException {
        super(conf);
    }

    @Override
    public String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJDBCUrlPrefix() {
        return "jdbc:postgresql:";
    }
}

package macrobase.ingest;

public class CachingPostgresLoader extends CachingSQLLoader {
    @Override
    public String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJDBCUrlPrefix() {
        return "jdbc:postgresql:";
    }
}

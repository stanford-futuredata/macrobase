package macrobase.ingest;

public class MemoryCachingPostgresLoader extends MemoryCachingSQLLoader {
    @Override
    public String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJDBCUrlPrefix() {
        return "jdbc:postgresql:";
    }
}

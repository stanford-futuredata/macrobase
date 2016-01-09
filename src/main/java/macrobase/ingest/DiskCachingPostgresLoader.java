package macrobase.ingest;

public class DiskCachingPostgresLoader extends DiskCachingSQLLoader {
    public DiskCachingPostgresLoader(String fileDir) {
        super(fileDir);
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

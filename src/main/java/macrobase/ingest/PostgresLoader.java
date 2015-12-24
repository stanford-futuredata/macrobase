package macrobase.ingest;

/**
 * Created by pbailis on 12/24/15.
 */
public class PostgresLoader extends SQLLoader {
    @Override
    public String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJDBCUrlPrefix() {
        return "jdbc:postgresql:";
    }
}

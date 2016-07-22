package macrobase.ingest;

import com.google.common.collect.Lists;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.runtime.resources.RowSetResource;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.StringJoiner;

public class BigDAWGIngester extends DataIngester {
    private static final Logger log = LoggerFactory.getLogger(BigDAWGIngester.class);

    private Connection connection;
    private final String dbUrl;
    private String dbName;
    private final Integer timeColumn;

    protected final String baseQuery;
    protected ResultSet resultSet;


    private final MBStream<Datum> output = new MBStream<>();
    private boolean connected = false;

    private static final String LIMIT_REGEX = "(LIMIT\\s\\d+)";

    public BigDAWGIngester(MacroBaseConf conf) throws ConfigurationException, SQLException {
        this(conf, null);
    }

    @Override
    public String getBaseQuery() {
        return null;
    }

    public BigDAWGIngester(MacroBaseConf conf, Connection connection) throws ConfigurationException, SQLException {
        super(conf);

        baseQuery = conf.getString(MacroBaseConf.BASE_QUERY);
        dbUrl = conf.getString(MacroBaseConf.DB_URL, MacroBaseDefaults.DB_URL);
        timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);

        if (connection != null) {
            this.connection = connection;
        }
    }

    public void connect() throws ConfigurationException, SQLException {
        //initializeResultSet();

        // while(!resultSet.isLast()) {
        //     output.add(getNext());
        // }
    }

    private String removeLimit(String sql) {
        return sql.replaceAll(LIMIT_REGEX, "");
    }

    private String removeSqlJunk(String sql) {
        return sql.replaceAll(";", "");
    }

    @Override
    public Schema getSchema(String baseQuery) throws Exception {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(dbUrl);

        String bigdawgSql = String.format("bdrel(%s LIMIT 1);", removeSqlJunk(removeLimit(baseQuery)));

        log.debug("bigdawg query: {}", bigdawgSql);
        StringEntity body = new StringEntity(bigdawgSql);

        httpPost.setEntity(body);
        CloseableHttpResponse response = httpclient.execute(httpPost);
        log.debug("{}", response.toString());

        InputStream responseStream = response.getEntity().getContent();
        log.debug("responseStream: {}", responseStream.read());
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(bigdawgSql);

        List<Schema.SchemaColumn> columns = Lists.newArrayList();

        for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            columns.add(new Schema.SchemaColumn(rs.getMetaData().getColumnName(i),
                    rs.getMetaData().getColumnTypeName(i)));
        }

        return new Schema(columns);
    }

    public RowSet getRows(String baseQuery,
                          List<RowSetResource.RowSetRequest.RowRequestPair> preds,
                          int limit,
                          int offset) throws SQLException {
        // initializeConnection();
        // TODO handle time column here
        Statement stmt = connection.createStatement();
        String sql = removeSqlJunk(removeLimit(baseQuery));

        if (preds.size() > 0) {
            StringJoiner sj = new StringJoiner(" AND ");
            preds.stream().forEach(e -> sj.add(String.format("%s = '%s'", e.column, e.value)));

            if (!sql.toLowerCase().contains("where")) {
                sql += " WHERE ";
            } else {
                sql += " AND ";
            }

            sql += sj.toString();
        }

        sql += String.format(" LIMIT %d OFFSET %d", limit, offset);

        ResultSet rs = stmt.executeQuery(sql);

        List<RowSet.Row> rows = Lists.newArrayList();
        while (rs.next()) {
            List<ColumnValue> columnValues = Lists.newArrayList();

            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                columnValues.add(
                        new ColumnValue(rs.getMetaData().getColumnName(i),
                                rs.getString(i)));
            }
            rows.add(new RowSet.Row(columnValues));
        }

        return new RowSet(rows);
    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return null;
    }
}


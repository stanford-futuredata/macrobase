package macrobase.ingest;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.server.resources.RowSetResource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PostgresLoader {
    private Connection connection;

    private String removeLimit(String sql) {
        return sql.replaceAll("LIMIT\\s\\d+", "");
    }

    private String removeSqlJunk(String sql) {
        return sql.replaceAll(";", "").replaceAll("'", "''");
    }

    public PostgresLoader() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    }

    public void connect(String pgUrl) throws SQLException {
        connection = DriverManager.getConnection("jdbc:postgresql:"+pgUrl);
    }

    public Schema getSchema(String baseQuery)
            throws SQLException {
        Statement stmt = connection.createStatement();
        String sql = String.format("%s LIMIT 1", removeSqlJunk(removeLimit(baseQuery)));
        ResultSet rs = stmt.executeQuery(sql);

        List<Schema.SchemaColumn> columns = Lists.newArrayList();

        for(int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            columns.add(new Schema.SchemaColumn(rs.getMetaData().getColumnName(i),
                                                rs.getMetaData().getColumnTypeName(i)));
        }

        return new Schema(columns);
    }

    public RowSet getRows(String baseQuery,
                          List<RowSetResource.RowSetRequest.RowRequestPair> preds,
                          int limit,
                          int offset) throws SQLException {
        Statement stmt = connection.createStatement();
        String sql = removeSqlJunk(removeLimit(baseQuery));

        if(preds.size() > 0) {
            StringJoiner sj = new StringJoiner(" AND ");
            preds.stream().forEach(e -> sj.add(String.format("%s = '%s'", e.column, e.value)));

            if(!sql.contains("WHERE")) {
                sql += " WHERE ";
            }

            sql += sj.toString();
        }

        sql += String.format(" LIMIT %d OFFSET %d", limit, offset);

        ResultSet rs = stmt.executeQuery(sql);

        List<RowSet.Row> rows = Lists.newArrayList();
        while(rs.next()) {
            List<ColumnValue> columnValues = Lists.newArrayList();

            for(int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                columnValues.add(
                        new ColumnValue(rs.getMetaData().getColumnName(i),
                        rs.getString(i)));
            }
            rows.add(new RowSet.Row(columnValues));
        }

        return new RowSet(rows);
    }

    public List<Datum> getData(DatumEncoder encoder,
                               List<String> attributes,
                               List<String> lowMetrics,
                               List<String> highMetrics,
                               String baseQuery) throws SQLException {

        String targetColumns = StreamSupport.stream(
                Iterables.concat(attributes, lowMetrics, highMetrics).spliterator(), false)
                .collect(Collectors.joining(", "));
        String sql = String.format("SELECT %s FROM (%s) baseQuery",
                                   targetColumns,
                                   removeSqlJunk(baseQuery));
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);


        for(int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            encoder.recordAttributeName(i, rs.getMetaData().getColumnName(i));
        }

        List<Datum> ret = Lists.newArrayList();

        while(rs.next()) {
            List<Integer> attrList = new ArrayList<>(attributes.size());

            int i = 1;
            for(; i <= attributes.size(); ++i) {
                attrList.add(encoder.getIntegerEncoding(i-1, rs.getString(i)));
            }

            List<Double> metricList = new ArrayList<>(lowMetrics.size()+highMetrics.size());

            for(; i <= attributes.size()+lowMetrics.size(); ++i) {
                metricList.add(Math.pow(Math.max(rs.getDouble(i), 0.1), -1));
            }

            for(; i <= attributes.size()+lowMetrics.size()+highMetrics.size(); ++i) {
                metricList.add(rs.getDouble(i));
            }

            ret.add(new Datum(attrList, metricList));
        }

        return ret;
    }

}

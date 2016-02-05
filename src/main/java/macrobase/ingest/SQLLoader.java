package macrobase.ingest;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.ManagedDataSource;
import macrobase.MacroBase;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.runtime.resources.RowSetResource;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class SQLLoader {
    abstract public String getDriverClass();
    abstract public String getJDBCUrlPrefix();

    @SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(SQLLoader.class);

    private ManagedDataSource source;

    private Connection connection;
    private String dbUser;
    private String dbPassword;

    private String removeLimit(String sql) {
        return sql.replaceAll("LIMIT\\s\\d+", "");
    }

    private String removeSqlJunk(String sql) {
        return sql.replaceAll(";", "").replaceAll("'", "''");
    }

    public void setDatabaseCredentials(String user, String password) {
        this.dbUser = user;
        this.dbPassword = password;
    }

    public void connect(String pgUrl) throws SQLException {
        DataSourceFactory factory = new DataSourceFactory();

        factory.setDriverClass(getDriverClass());
        factory.setUrl(getJDBCUrlPrefix()+pgUrl);

        if (this.dbUser != null) {
            factory.setUser(this.dbUser);
        }
        if (this.dbPassword != null) {
            factory.setPassword(this.dbPassword);
        }

        source = factory.build(MacroBase.metrics, "postgres");
        connection = source.getConnection();
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

            if(!sql.toLowerCase().contains("where")) {
                sql += " WHERE ";
            } else {
                sql += " AND ";
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
                               String baseQuery) throws SQLException, IOException {

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

        RealVector metricWiseMinVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
        RealVector metricWiseMaxVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());

        while(rs.next()) {
            List<Integer> attrList = new ArrayList<>(attributes.size());

            int i = 1;
            for(; i <= attributes.size(); ++i) {
                attrList.add(encoder.getIntegerEncoding(i, rs.getString(i)));
            }

            RealVector metricVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
            int vecPos = 0;

            for(; i <= attributes.size() + lowMetrics.size(); ++i) {
                double val = Math.pow(Math.max(rs.getDouble(i), 0.1), -1);
                metricVec.setEntry(vecPos, val);

                if(metricWiseMinVec.getEntry(vecPos) > val) {
                    metricWiseMinVec.setEntry(vecPos, val);
                }

                if(metricWiseMaxVec.getEntry(vecPos) < val) {
                    metricWiseMaxVec.setEntry(vecPos, val);
                }

                vecPos += 1;
            }

            for(; i <= attributes.size() + lowMetrics.size() + highMetrics.size(); ++i) {
                double val = rs.getDouble(i);
                metricVec.setEntry(vecPos, val);

                if(metricWiseMinVec.getEntry(vecPos) > val) {
                    metricWiseMinVec.setEntry(vecPos, val);
                }

                if(metricWiseMaxVec.getEntry(vecPos) < val) {
                    metricWiseMaxVec.setEntry(vecPos, val);
                }

                vecPos += 1;
            }

            ret.add(new Datum(attrList, metricVec));
        }

        // normalize data
        for(Datum d : ret) {
            // ebeDivide returns a copy; avoid a copy at the expense of ugly code
            RealVector metrics = d.getMetrics();
            for(int dim = 0; dim < metrics.getDimension(); ++dim) {
                double dimMin = metricWiseMinVec.getEntry(dim);
                double dimMax = metricWiseMaxVec.getEntry(dim);

                if(dimMax - dimMin == 0) {
                    log.warn("No difference between min and max in dimension {}!", dim);
                    metrics.setEntry(dim, 0);
                    continue;
                }

                double cur = metrics.getEntry(dim);
                metrics.setEntry(dim, (cur - dimMin)/(dimMax - dimMin));
            }
        }

        return ret;
    }

}
package macrobase.ingest;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.ManagedDataSource;
import macrobase.MacroBase;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.ingest.transform.DataTransformation;
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

public abstract class SQLLoader extends DataLoader{
    abstract public String getDriverClass();
    abstract public String getJDBCUrlPrefix();

    private ManagedDataSource source;

    private final Connection connection;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String dbName;

    protected final String baseQuery;

    public SQLLoader(MacroBaseConf conf) throws ConfigurationException, SQLException {
        super(conf);

        dbUser = conf.getString(MacroBaseConf.DB_USER, MacroBaseDefaults.DB_USER);
        dbPassword = conf.getString(MacroBaseConf.DB_PASSWORD, MacroBaseDefaults.DB_PASSWORD);
        dbName = conf.getString(MacroBaseConf.DB_NAME, MacroBaseDefaults.DB_NAME);
        baseQuery = conf.getString(MacroBaseConf.BASE_QUERY);
        dbUrl = conf.getString(MacroBaseConf.DB_URL, MacroBaseDefaults.DB_URL);

        System.out.println(dbUrl+" "+dbName+" "+getJDBCUrlPrefix()+dbUrl);

        DataSourceFactory factory = new DataSourceFactory();

        factory.setDriverClass(getDriverClass());
        factory.setUrl(String.format("%s//%s/%s", getJDBCUrlPrefix(), dbUrl, dbName));

        if (dbUser != null) {
            factory.setUser(this.dbUser);
        }
        if (dbPassword != null) {
            factory.setPassword(dbPassword);
        }

        source = factory.build(MacroBase.metrics, dbName);
        connection = source.getConnection();
    }

    private String removeLimit(String sql) {
        return sql.replaceAll("LIMIT\\s\\d+", "");
    }

    private String removeSqlJunk(String sql) {
        return sql.replaceAll(";", "");
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

    @Override
    public List<Datum> getData(DatumEncoder encoder)
        throws SQLException, IOException {

        String targetColumns = StreamSupport.stream(
                Iterables.concat(attributes, lowMetrics, highMetrics, auxiliaryAttributes).spliterator(), false)
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
                attrList.add(encoder.getIntegerEncoding(i, rs.getString(i)));
            }

            RealVector metricVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
            int vecPos = 0;

            for(; i <= attributes.size() + lowMetrics.size(); ++i) {
                double val = Math.pow(Math.max(rs.getDouble(i), 0.1), -1);
                metricVec.setEntry(vecPos, val);

                vecPos += 1;
            }

            for(; i <= attributes.size() + lowMetrics.size() + highMetrics.size(); ++i) {
                double val = rs.getDouble(i);
                metricVec.setEntry(vecPos, val);

                vecPos += 1;
            }

            Datum datum = new Datum(attrList, metricVec);

            // Set auxilaries on the datum if user specified
            if (auxiliaryAttributes.size() > 0) {
                RealVector auxilaries = new ArrayRealVector(auxiliaryAttributes.size());
                for (int j=0; j < auxiliaryAttributes.size(); ++j, ++i) {
                    auxilaries.setEntry(j, rs.getDouble(i));
                }
                datum.setAuxiliaries(auxilaries);
            }

            ret.add(datum);
        }

        // normalize data
        dataTransformation.transform(ret);

        return ret;
    }

}

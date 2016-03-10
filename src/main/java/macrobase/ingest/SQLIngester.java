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
import macrobase.datamodel.TimeDatum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.runtime.resources.RowSetResource;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class SQLIngester extends DataIngester {
    private static final Logger log = LoggerFactory.getLogger(SQLIngester.class);

    abstract public String getDriverClass();

    abstract public String getJDBCUrlPrefix();

    private ManagedDataSource source;

    private Connection connection;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String dbName;

    protected final String baseQuery;
    protected ResultSet resultSet;
    
    private static final String LIMIT_REGEX = "(LIMIT\\s\\d+)";

    public SQLIngester(MacroBaseConf conf) throws ConfigurationException, SQLException {
        this(conf, null);
    }

    public SQLIngester(MacroBaseConf conf, Connection connection) throws ConfigurationException, SQLException {
        super(conf);

        dbUser = conf.getString(MacroBaseConf.DB_USER, MacroBaseDefaults.DB_USER);
        dbPassword = conf.getString(MacroBaseConf.DB_PASSWORD, MacroBaseDefaults.DB_PASSWORD);
        dbName = conf.getString(MacroBaseConf.DB_NAME, MacroBaseDefaults.DB_NAME);
        baseQuery = conf.getString(MacroBaseConf.BASE_QUERY);
        dbUrl = conf.getString(MacroBaseConf.DB_URL, MacroBaseDefaults.DB_URL);

        if(connection != null) {
            this.connection = connection;
        }
    }

    private String removeLimit(String sql) {
    	return sql.replaceAll(LIMIT_REGEX, "");
    }

    private String removeSqlJunk(String sql) {
        return sql.replaceAll(";", "");
    }
    
    public Schema getSchema(String baseQuery)
            throws SQLException {
        initializeConnection();

        Statement stmt = connection.createStatement();
        String sql = String.format("%s LIMIT 1", removeSqlJunk(removeLimit(baseQuery)));
        ResultSet rs = stmt.executeQuery(sql);

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
        initializeConnection();
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

    private void initializeConnection() throws SQLException {
        if(connection == null) {
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
            this.connection = source.getConnection();
        }
    }

    private void initializeResultSet() throws SQLException {
        initializeConnection();

        if(resultSet == null) {
            String targetColumns = StreamSupport.stream(
                    Iterables.concat(attributes, lowMetrics, highMetrics, contextualDiscreteAttributes,
                                     contextualDoubleAttributes, auxiliaryAttributes).spliterator(), false)
                    .collect(Collectors.joining(", "));
            if (timeColumn != null) {
                targetColumns += ", " + timeColumn;
            }
            String sql = String.format("SELECT %s FROM (%s) baseQuery",
                                       targetColumns,
                                       orderByTimeColumn(removeSqlJunk(baseQuery), timeColumn));
            if (timeColumn != null) {
                // Both nested and outer query need to be ordered
                sql += " ORDER BY " + timeColumn;
            }
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sql);

            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
                conf.getEncoder().recordAttributeName(i, resultSet.getMetaData().getColumnName(i));
            }
        }
    }

    @Override
    public boolean hasNext() {
        try {
            initializeResultSet();
            return !resultSet.isLast();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Datum next() {
        try {
            initializeResultSet();
            return getNext();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new NoSuchElementException();
        }
    }

    private Datum getNext() throws SQLException {
        resultSet.next();
        List<Integer> attrList = getAttrs(resultSet, conf.getEncoder(), 1);
        RealVector metricVec = getMetrics(resultSet, attrList.size() + 1);

        List<Integer> contextualDiscreteAttrValues = getContextualDiscreteAttrs(resultSet,conf.getEncoder(),attrList.size() + metricVec.getDimension() + 1);
        RealVector contextualDoubleAttrValues = getContextualDoubleAttrs(resultSet,attrList.size() + metricVec.getDimension() + contextualDiscreteAttrValues.size()+ 1);

        Datum datum;
        if (timeColumn == null) {
            datum = new Datum(attrList, metricVec, contextualDiscreteAttrValues, contextualDoubleAttrValues);
        } else {
            datum = new TimeDatum(resultSet.getInt(timeColumn), attrList, metricVec, contextualDiscreteAttrValues, contextualDoubleAttrValues);
        }

        // Set auxilaries on the datum if user specified
        if (auxiliaryAttributes.size() > 0) {
        	RealVector auxilaries = getAuxiliaries(resultSet, attrList.size() + metricVec.getDimension() + contextualDiscreteAttrValues.size() + contextualDoubleAttrValues.getDimension() + 1);
            datum.setAuxiliaries(auxilaries);
        }
        return datum;
    }
    
    private List<Integer> getAttrs(ResultSet rs, DatumEncoder encoder, int rsStartIndex) throws SQLException {
    	List<Integer> attrList = new ArrayList<>(attributes.size());
        for (int i = rsStartIndex; i <= attributes.size(); ++i) {
            attrList.add(encoder.getIntegerEncoding(i, rs.getString(i)));
        }
        return attrList;
	}

	private RealVector getMetrics(ResultSet rs, int rsStartIndex)
			throws SQLException {
		RealVector metricVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
		int vecPos = 0;

		for (int i = 0; i < lowMetrics.size(); ++i, ++vecPos) {
		    double val = Math.pow(Math.max(rs.getDouble(i + rsStartIndex), 0.1), -1);
		    metricVec.setEntry(vecPos, val);
		}
		
		rsStartIndex += lowMetrics.size();

		for (int i = 0; i < highMetrics.size(); ++i, ++vecPos) {
		    double val = rs.getDouble(i + rsStartIndex);
		    metricVec.setEntry(vecPos, val);
		}
		return metricVec;
	}

	private RealVector getAuxiliaries(ResultSet rs,
			int rsStartIndex) throws SQLException {
		RealVector auxilaries = new ArrayRealVector(auxiliaryAttributes.size());
		for (int i = 0; i < auxiliaryAttributes.size(); ++i) {
		    auxilaries.setEntry(i, rs.getDouble(i + rsStartIndex));
		}
		return auxilaries;
	}
	
	private List<Integer> getContextualDiscreteAttrs(ResultSet rs, DatumEncoder encoder, int rsStartIndex) throws SQLException{
		 List<Integer> contextualDiscreteAttributesValues = new ArrayList<>(contextualDiscreteAttributes.size());
         for (int i = 0; i < contextualDiscreteAttributes.size(); ++i) {
         	contextualDiscreteAttributesValues.add(encoder.getIntegerEncoding(i + rsStartIndex, rs.getString(i + rsStartIndex)));
         }
         return contextualDiscreteAttributesValues;
	}
	private RealVector getContextualDoubleAttrs(ResultSet rs, int rsStartIndex) throws SQLException{
		  RealVector contextualDoubleAttributesValues = new ArrayRealVector(contextualDoubleAttributes.size());
         
          for (int i = 0 ; i <  contextualDoubleAttributes.size(); ++i) {
              double val = rs.getDouble(i + rsStartIndex);
              contextualDoubleAttributesValues.setEntry(i, val);

          }
          return contextualDoubleAttributesValues;
	}
	

	// Shield your eyes, mere mortals, from this glorious hideousness.
    private String orderByTimeColumn(String sql, @Nullable String timeColumn) {
        if (timeColumn == null) {
            return sql;
        } else {
            if (sql.toLowerCase().contains("order by")) {
                throw new RuntimeException("baseQuery currently shouldn't contain ORDER BY if timeColumn is specified.");
            }

            String orderBy = " ORDER BY " + timeColumn;
            if (Pattern.compile(LIMIT_REGEX).matcher(sql).find()) {
                return sql.replaceAll(LIMIT_REGEX, orderBy + " $1");
            } else {
                return sql + orderBy;
            }
        }
    }

    @Override
    public String getBaseQuery() {
        return baseQuery;
    }

}

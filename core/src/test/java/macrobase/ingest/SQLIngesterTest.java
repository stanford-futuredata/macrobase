package macrobase.ingest;

import com.google.common.collect.Lists;
import com.mockrunner.jdbc.StatementResultSetHandler;
import com.mockrunner.mock.jdbc.JDBCMockObjectFactory;
import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SQLIngesterTest {
    public class TestSQLIngester extends SQLIngester {
        public TestSQLIngester(MacroBaseConf conf, Connection connection)
                throws ConfigurationException, SQLException {
            super(conf, connection);
        }

        @Override
        public String getDriverClass() {
            return "fake";
        }

        @Override
        public String getJDBCUrlPrefix() {
            return "fake";
        }
    }

    @Test
    public void testJDBCProperties() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");
        conf.set(MacroBaseConf.JDBC_PROPERTIES, "{sslfactory=org.postgresql.ssl.NonValidatingFactory, ssl=true}");
        Map<String, String> properties = new PostgresIngester(conf).getJDBCProperties();
        assertEquals("org.postgresql.ssl.NonValidatingFactory", properties.get("sslfactory"));
        assertEquals("true", properties.get("ssl"));
    }

    @Test
    public void testSchema() throws Exception {
        JDBCMockObjectFactory factory = new JDBCMockObjectFactory();
        factory.registerMockDriver();
        MockConnection connection = factory.getMockConnection();
        StatementResultSetHandler statementHandler =
                connection.getStatementResultSetHandler();
        MockResultSet result = statementHandler.createResultSet();
        result.setFetchSize(0);

        MockResultSetMetaData metaData = new MockResultSetMetaData();

        Map<String, String> fakeColumnMap = new HashMap<>();
        fakeColumnMap.put("test1", "numeric");
        fakeColumnMap.put("test2", "varchar");
        fakeColumnMap.put("test3", "int8");

        Set<String> fakeColumns = fakeColumnMap.keySet();

        metaData.setColumnCount(fakeColumnMap.size());
        int i = 1;
        for (Map.Entry<String, String> fc : fakeColumnMap.entrySet()) {
            metaData.setColumnName(i, fc.getKey());
            metaData.setColumnTypeName(i, fc.getValue());
            i++;
        }

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList(fakeColumns));
        conf.set(MacroBaseConf.METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");


        SQLIngester loader = new TestSQLIngester(conf, connection);
        Schema schema = loader.getSchema("foo");

        for (Schema.SchemaColumn sc : schema.getColumns()) {
            assertTrue(fakeColumns.contains(sc.getName()));
            assertEquals(fakeColumnMap.get(sc.getName()), sc.getType());
            fakeColumns.remove(sc.getName());
        }

        assertTrue(fakeColumns.isEmpty());
    }


    @Test
    public void testGetData() throws Exception {
        JDBCMockObjectFactory factory = new JDBCMockObjectFactory();
        factory.registerMockDriver();
        MockConnection connection = factory.getMockConnection();
        StatementResultSetHandler statementHandler =
                connection.getStatementResultSetHandler();
        MockResultSet result = statementHandler.createResultSet();

        MockResultSetMetaData metaData = new MockResultSetMetaData();

        List<Integer> fakeData = new ArrayList<>();

        final int NUM_ROWS = 100;
        final int NUM_ATTRS = 5;
        final int NUM_HIGH = 2;
        final int NUM_LOW = 3;
        final int NUM_AUXILIARY = 1;

        final int DIMENSION = NUM_ATTRS + NUM_HIGH + NUM_LOW + NUM_AUXILIARY;

        Integer val = 1;
        Set<String> firstVals = new HashSet<>();
        for(int rno = 0; rno < NUM_ROWS; ++rno) {
            List<Object> rowString = new ArrayList<>();
            firstVals.add(val.toString());
            for(Integer i = 0; i < DIMENSION; ++i) {
                rowString.add(val.toString());
                val++;
            }
            result.addRow(rowString);
        }

        int column = 1;
        List<String> attributes = new ArrayList<>();
        for(int i = 0; i < NUM_ATTRS; ++i) {
            String attrName = String.format("attr%d", i);
            metaData.setColumnName(column, attrName);
            attributes.add(attrName);
            column++;
        }

        List<String> lowMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_LOW; ++i) {
            String metricName = String.format("lowMetric%d", i);
            metaData.setColumnName(column, metricName);
            lowMetrics.add(metricName);
            column++;
        }

        List<String> highMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_HIGH; ++i) {
            String metricName = String.format("highMetric%d", i);
            metaData.setColumnName(column, metricName);
            highMetrics.add(metricName);
            column++;
        }

        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        List<String> allMetrics = new ArrayList<>();
        allMetrics.addAll(lowMetrics);
        allMetrics.addAll(highMetrics);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.METRICS, allMetrics);

        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");

        DatumEncoder encoder = conf.getEncoder();
        SQLIngester ingester = new TestSQLIngester(conf, connection);
        List<Datum> data = ingester.getStream().drain();

        for (Datum d : data) {
            String firstValString = encoder.getAttribute(d.attributes().get(0)).getValue();
            Integer curValInt = Integer.parseInt(firstValString);
            assertTrue(firstVals.contains(firstValString));
            firstVals.remove(firstValString);

            column = 0;
            for(int i = 0; i < NUM_ATTRS; ++i) {
                assertEquals(curValInt,
                             (Integer) Integer.parseInt(encoder.getAttribute(d.attributes().get(i)).getValue()));
                curValInt++;
                column++;
            }

            for(int i = 0; i < NUM_LOW+NUM_HIGH; ++i) {
                assertEquals(curValInt, d.metrics().getEntry(i), 0);
                curValInt++;
            }
        }

        assertTrue(firstVals.isEmpty());
    }

    @Test
    public void testGetData2() throws Exception {
        JDBCMockObjectFactory factory = new JDBCMockObjectFactory();
        factory.registerMockDriver();
        MockConnection connection = factory.getMockConnection();
        StatementResultSetHandler statementHandler =
                connection.getStatementResultSetHandler();
        MockResultSet result = statementHandler.createResultSet();

        MockResultSetMetaData metaData = new MockResultSetMetaData();

        List<Integer> fakeData = new ArrayList<>();

        final int NUM_ROWS = 100;
        final int NUM_ATTRS = 5;
        final int NUM_HIGH = 2;
        final int NUM_LOW = 3;
        final int NUM_AUXILIARY = 1;
        final int NUM_CONTEXTUAL_DISCRETE = 1;
        final int NUM_CONTEXTUAL_DOUBLE = 1;

        final int DIMENSION = NUM_ATTRS + NUM_HIGH + NUM_LOW  + NUM_CONTEXTUAL_DISCRETE + NUM_CONTEXTUAL_DOUBLE + NUM_AUXILIARY;

        Integer val = 1;
        Set<String> firstVals = new HashSet<>();
        for(int rno = 0; rno < NUM_ROWS; ++rno) {
            List<Object> rowString = new ArrayList<>();
            firstVals.add(val.toString());
            for(Integer i = 0; i < DIMENSION; ++i) {
                rowString.add(val.toString());
                val++;
            }
            result.addRow(rowString);
        }

        int column = 1;
        List<String> attributes = new ArrayList<>();
        for(int i = 0; i < NUM_ATTRS; ++i) {
            String attrName = String.format("attr%d", i);
            metaData.setColumnName(column, attrName);
            attributes.add(attrName);
            column++;
        }

        List<String> lowMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_LOW; ++i) {
            String metricName = String.format("lowMetric%d", i);
            metaData.setColumnName(column, metricName);
            lowMetrics.add(metricName);
            column++;
        }

        List<String> highMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_HIGH; ++i) {
            String metricName = String.format("highMetric%d", i);
            metaData.setColumnName(column, metricName);
            highMetrics.add(metricName);
            column++;
        }

        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        List<String> allMetrics = new ArrayList<>();
        allMetrics.addAll(lowMetrics);
        allMetrics.addAll(highMetrics);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.METRICS, allMetrics);
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");

        DatumEncoder encoder = conf.getEncoder();
        SQLIngester ingester = new TestSQLIngester(conf, connection);
        List<Datum> data = ingester.getStream().drain();

        for (Datum d : data) {
            String firstValString = encoder.getAttribute(d.attributes().get(0)).getValue();
            Integer curValInt = Integer.parseInt(firstValString);
            assertTrue(firstVals.contains(firstValString));
            firstVals.remove(firstValString);

            column = 0;
            for(int i = 0; i < NUM_ATTRS; ++i) {
                assertEquals(curValInt,
                             (Integer) Integer.parseInt(encoder.getAttribute(d.attributes().get(i)).getValue()));
                curValInt++;
                column++;
            }

            for(int i = 0; i < NUM_LOW+NUM_HIGH; ++i) {
                assertEquals(curValInt, d.metrics().getEntry(i), 0);
                curValInt++;
            }
        }

        assertTrue(firstVals.isEmpty());
    }

    @Test
    public void testGetRows() throws Exception {
        JDBCMockObjectFactory factory = new JDBCMockObjectFactory();
        factory.registerMockDriver();
        MockConnection connection = factory.getMockConnection();
        StatementResultSetHandler statementHandler =
                connection.getStatementResultSetHandler();
        MockResultSet result = statementHandler.createResultSet();

        MockResultSetMetaData metaData = new MockResultSetMetaData();

        List<Integer> fakeData = new ArrayList<>();

        final int NUM_ROWS = 100;
        final int NUM_ATTRS = 5;
        final int NUM_HIGH = 2;
        final int NUM_LOW = 3;
        final int NUM_AUXILIARY = 1;

        final int DIMENSION = NUM_ATTRS + NUM_HIGH + NUM_LOW + NUM_AUXILIARY;

        Integer val = 1;
        for(int rno = 0; rno < NUM_ROWS; ++rno) {
            List<Object> rowString = new ArrayList<>();
            for(Integer i = 0; i < DIMENSION; ++i) {
                rowString.add(val.toString());
                val++;
            }
            result.addRow(rowString);
        }

        int column = 1;
        List<String> attributes = new ArrayList<>();
        for(int i = 0; i < NUM_ATTRS; ++i) {
            String attrName = String.format("attr%d", i);
            metaData.setColumnName(column, attrName);
            attributes.add(attrName);
            column++;
        }

        List<String> lowMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_LOW; ++i) {
            String metricName = String.format("lowMetric%d", i);
            metaData.setColumnName(column, metricName);
            lowMetrics.add(metricName);
            column++;
        }

        List<String> highMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_HIGH; ++i) {
            String metricName = String.format("highMetric%d", i);
            metaData.setColumnName(column, metricName);
            highMetrics.add(metricName);
            column++;
        }

        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        List<String> allMetrics = new ArrayList<>();
        allMetrics.addAll(lowMetrics);
        allMetrics.addAll(highMetrics);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.METRICS, allMetrics);
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test LIMIT 10000;");

        SQLIngester loader = new TestSQLIngester(conf, connection);
        loader.connect();

        Map<String, String> preds = new HashMap<>();

        preds.put("c1", "v1");

        RowSet rs = loader.getRows(conf.getString(MacroBaseConf.BASE_QUERY),
                                       preds,
                                       100,
                                       1000);

        assertEquals(NUM_ROWS, rs.getRows().size());

        int curVal = 1;
        for (RowSet.Row r : rs.getRows()) {
            for(ColumnValue cv : r.getColumnValues()) {
                assertEquals(curVal, Integer.parseInt(cv.getValue()));
                curVal++;
            }
        }
    }
}

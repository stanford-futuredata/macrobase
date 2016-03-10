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
import macrobase.runtime.resources.RowSetResource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SQLIngesterTest {
    private static final Logger log = LoggerFactory.getLogger(SQLIngesterTest.class);

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
    public void testSchema() throws Exception {
        JDBCMockObjectFactory factory = new JDBCMockObjectFactory();
        factory.registerMockDriver();
        MockConnection connection = factory.getMockConnection();
        StatementResultSetHandler statementHandler =
                connection.getStatementResultSetHandler();
        MockResultSet result = statementHandler.createResultSet();

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
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, new ArrayList<>());
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

        List<String> auxiliaryAttributes = new ArrayList<>();
        for(int i = 0; i < NUM_AUXILIARY; ++i) {
            String metricName = String.format("auxiliary%d", i);
            metaData.setColumnName(column, metricName);
            auxiliaryAttributes.add(metricName);
            column++;
        }

        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.LOW_METRICS, lowMetrics);
        conf.set(MacroBaseConf.HIGH_METRICS, highMetrics);
        conf.set(MacroBaseConf.AUXILIARY_ATTRIBUTES, auxiliaryAttributes);

        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");

        DatumEncoder encoder = conf.getEncoder();
        List<Datum> data = Lists.newArrayList(new TestSQLIngester(conf, connection));

        for (Datum d : data) {
            String firstValString = encoder.getAttribute(d.getAttributes().get(0)).getValue();
            Integer curValInt = Integer.parseInt(firstValString);
            assertTrue(firstVals.contains(firstValString));
            firstVals.remove(firstValString);

            column = 0;
            for(int i = 0; i < NUM_ATTRS; ++i) {
                assertEquals(curValInt,
                             (Integer) Integer.parseInt(encoder.getAttribute(d.getAttributes().get(i)).getValue()));
                curValInt++;
                column++;
            }

            for(int i = 0; i < NUM_LOW; ++i) {
                assertEquals(Math.pow(Math.max(curValInt, 0.1), -1), d.getMetrics().getEntry(i), 0);
                curValInt++;
            }

            for(int i = NUM_LOW; i < NUM_LOW+NUM_HIGH; ++i) {
                assertEquals(curValInt, d.getMetrics().getEntry(i), 0);
                curValInt++;
            }

            assertEquals(curValInt, d.getAuxiliaries().getEntry(0), 0);
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

        List<String> auxiliaryAttributes = new ArrayList<>();
        for(int i = 0; i < NUM_AUXILIARY; ++i) {
            String metricName = String.format("auxiliary%d", i);
            metaData.setColumnName(column, metricName);
            auxiliaryAttributes.add(metricName);
            column++;
        }

        List<String> contextualDiscreteAttributes = new ArrayList<>();
        for(int i = 0; i < NUM_CONTEXTUAL_DISCRETE; ++i) {
            String metricName = String.format("contextualDiscrete%d", i);
            metaData.setColumnName(column, metricName);
            contextualDiscreteAttributes.add(metricName);
            column++;
        }

        List<String> contextualDoubleAttributes = new ArrayList<>();
        for(int i = 0; i < NUM_CONTEXTUAL_DISCRETE; ++i) {
            String metricName = String.format("contextualDouble%d", i);
            metaData.setColumnName(column, metricName);
            contextualDoubleAttributes.add(metricName);
            column++;
        }
        
        
        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.LOW_METRICS, lowMetrics);
        conf.set(MacroBaseConf.HIGH_METRICS, highMetrics);
        conf.set(MacroBaseConf.AUXILIARY_ATTRIBUTES, auxiliaryAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, contextualDiscreteAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, contextualDoubleAttributes);
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");

        DatumEncoder encoder = conf.getEncoder();
        List<Datum> data = Lists.newArrayList(new TestSQLIngester(conf, connection));

        for (Datum d : data) {
            String firstValString = encoder.getAttribute(d.getAttributes().get(0)).getValue();
            Integer curValInt = Integer.parseInt(firstValString);
            assertTrue(firstVals.contains(firstValString));
            firstVals.remove(firstValString);

            column = 0;
            for(int i = 0; i < NUM_ATTRS; ++i) {
                assertEquals(curValInt,
                             (Integer) Integer.parseInt(encoder.getAttribute(d.getAttributes().get(i)).getValue()));
                curValInt++;
                column++;
            }

            for(int i = 0; i < NUM_LOW; ++i) {
                assertEquals(Math.pow(Math.max(curValInt, 0.1), -1), d.getMetrics().getEntry(i), 0);
                curValInt++;
            }

            for(int i = NUM_LOW; i < NUM_LOW+NUM_HIGH; ++i) {
                assertEquals(curValInt, d.getMetrics().getEntry(i), 0);
                curValInt++;
            }

            for(int i = 0; i < NUM_CONTEXTUAL_DISCRETE; ++i){
            	 assertEquals(curValInt, (Integer) Integer.parseInt(encoder.getAttribute(d.getContextualDiscreteAttributes().get(i)).getValue()));
                 curValInt++;
            }
            
            for(int i = 0; i < NUM_CONTEXTUAL_DOUBLE; ++i){
            	  assertEquals(curValInt, d.getContextualDoubleAttributes().getEntry(i), 0);
                  curValInt++;
            }
            
            for(int i = 0; i < NUM_AUXILIARY; ++i) {
            	assertEquals(curValInt, d.getAuxiliaries().getEntry(i), 0);
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

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.LOW_METRICS, lowMetrics);
        conf.set(MacroBaseConf.HIGH_METRICS, highMetrics);
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test LIMIT 10000;");

        SQLIngester loader = new TestSQLIngester(conf, connection);
        List<RowSetResource.RowSetRequest.RowRequestPair> pairs = new ArrayList<>();
        RowSetResource.RowSetRequest.RowRequestPair rrp = new RowSetResource.RowSetRequest.RowRequestPair();
        rrp.column = "c1";
        rrp.value = "v1";
        pairs.add(rrp);
        RowSet rs = loader.getRows(conf.getString(MacroBaseConf.BASE_QUERY),
                                       pairs,
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

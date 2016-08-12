package macrobase.ingest;

import com.google.common.collect.Sets;
import com.mockrunner.jdbc.StatementResultSetHandler;
import com.mockrunner.mock.jdbc.JDBCMockObjectFactory;
import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CachingSQLIngesterTest {
    private static final Logger log = LoggerFactory.getLogger(CachingSQLIngesterTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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

        Set<String> firstVals2 = Sets.newHashSet(firstVals);

        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        List<String> allMetrics = new ArrayList<>();
        allMetrics.addAll(lowMetrics);
        allMetrics.addAll(highMetrics);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.METRICS, allMetrics);
        conf.set(MacroBaseConf.DB_CACHE_DIR, folder.newFolder());

        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");

        DatumEncoder encoder = conf.getEncoder();
        List<Datum> data = new DiskCachingIngester(conf, new TestSQLIngester(conf, connection)).getStream().drain();

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

        DatumEncoder encoder2 = conf.getEncoder();
        SQLIngester t = new TestSQLIngester(conf, connection);
        t.connect();
        List<Datum> data2 = new DiskCachingIngester(conf, t).getStream().drain();

        for (Datum d : data2) {
            String firstValString = encoder2.getAttribute(d.attributes().get(0)).getValue();
            Integer curValInt = Integer.parseInt(firstValString);
            assertTrue(firstVals2.contains(firstValString));
            firstVals2.remove(firstValString);

            column = 0;
            for(int i = 0; i < NUM_ATTRS; ++i) {
                assertEquals(curValInt,
                             (Integer) Integer.parseInt(encoder2.getAttribute(d.attributes().get(i)).getValue()));
                curValInt++;
                column++;
            }

            for(int i = 0; i < NUM_LOW+NUM_HIGH; ++i) {
                assertEquals(curValInt, d.metrics().getEntry(i), 0);
                curValInt++;
            }
        }

        assertTrue(firstVals2.isEmpty());
    }


    @Test
    public void testChunkedIO() throws Exception {
        JDBCMockObjectFactory factory = new JDBCMockObjectFactory();
        factory.registerMockDriver();
        MockConnection connection = factory.getMockConnection();
        StatementResultSetHandler statementHandler =
                connection.getStatementResultSetHandler();
        MockResultSet result = statementHandler.createResultSet();

        MockResultSetMetaData metaData = new MockResultSetMetaData();

        List<Integer> fakeData = new ArrayList<>();

        final int NUM_ROWS = 1000;
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

        Set<String> firstVals2 = Sets.newHashSet(firstVals);

        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);

        List<String> allMetrics = new ArrayList<>();
        allMetrics.addAll(lowMetrics);
        allMetrics.addAll(highMetrics);

        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.METRICS, allMetrics);
        conf.set(MacroBaseConf.DB_CACHE_DIR, folder.newFolder());

        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");
        conf.set(MacroBaseConf.DB_CACHE_CHUNK_SIZE, 1);

        DatumEncoder encoder = conf.getEncoder();
        List<Datum> data = new DiskCachingIngester(conf, new TestSQLIngester(conf, connection)).getStream().drain();

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

        DatumEncoder encoder2 = conf.getEncoder();
        SQLIngester t = new TestSQLIngester(conf, connection);
        t.connect();
        List<Datum> data2 = new DiskCachingIngester(conf, t).getStream().drain();

        for (Datum d : data2) {
            String firstValString = encoder2.getAttribute(d.attributes().get(0)).getValue();
            Integer curValInt = Integer.parseInt(firstValString);
            assertTrue(firstVals2.contains(firstValString));
            firstVals2.remove(firstValString);

            column = 0;
            for(int i = 0; i < NUM_ATTRS; ++i) {
                assertEquals(curValInt,
                             (Integer) Integer.parseInt(encoder2.getAttribute(d.attributes().get(i)).getValue()));
                curValInt++;
                column++;
            }

            for(int i = 0; i < NUM_LOW+NUM_HIGH; ++i) {
                assertEquals(curValInt, d.metrics().getEntry(i), 0);
                curValInt++;
            }
        }

        assertTrue(firstVals2.isEmpty());
    }
}

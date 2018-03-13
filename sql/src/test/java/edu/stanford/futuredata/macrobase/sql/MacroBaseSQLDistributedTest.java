package edu.stanford.futuredata.macrobase.sql;

import edu.stanford.futuredata.macrobase.sql.parser.ParsingException;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import edu.stanford.futuredata.macrobase.sql.tree.DiffQuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.Statement;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class MacroBaseSQLDistributedTest {

    @Test
    public void testIngestFromCSV() throws Exception {
        final String queryStr = "IMPORT FROM CSV FILE \"src/test/resources/tiny.csv\" INTO tiny(usage string, version string, location string);";
        SqlParser parser = new SqlParser();
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("macrobase-sql-spark")
                .getOrCreate();
        QueryEngineDistributed queryEngineDistributed = new QueryEngineDistributed(spark, 1);
        final Statement stmt;
        try {
            stmt = parser.createStatement(queryStr.replace(";", ""));
            assertTrue( "ingestQuery should generate a Statement of type Query",
                    stmt instanceof ImportCsv);
        } catch (ParsingException e) {
            e.printStackTrace();
            throw new Error("ingestQuery should parse");
        }
        final ImportCsv importStatement = (ImportCsv) stmt;

        Dataset<Row> result = queryEngineDistributed.importTableFromCsv(importStatement);

        List<Row> collectedResult = result.collectAsList();

        collectedResult.sort((Row recordOne, Row recordTwo) -> {
            Double doubleOne = Double.parseDouble(recordOne.getString(0));
            Double doubleTwo = Double.parseDouble(recordTwo.getString(0));

        if (doubleOne > doubleTwo)
                return 1;
        else if (doubleOne.equals(doubleTwo))
            return 0;
        else
            return -1;});

        assertEquals(3, collectedResult.size());

        assertEquals("2.0", collectedResult.get(0).getString(0));
        assertEquals("27", collectedResult.get(0).getString(1));
        assertEquals("USA", collectedResult.get(0).getString(2));
        assertEquals("3.1", collectedResult.get(1).getString(0));
        assertEquals("27", collectedResult.get(1).getString(1));
        assertEquals("CAN", collectedResult.get(1).getString(2));
        assertEquals("4.0", collectedResult.get(2).getString(0));
        assertEquals("28", collectedResult.get(2).getString(1));
        assertEquals("USA", collectedResult.get(2).getString(2));

        spark.stop();
    }

    @Test
    public void testDiffBasic() throws Exception {
        final String importStr = "IMPORT FROM CSV FILE \"../core/demo/sample.csv\" INTO sample(usage string, latency string, location string, version string);";
        SqlParser parser = new SqlParser();
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("macrobase-sql-spark")
                .getOrCreate();
        QueryEngineDistributed queryEngineDistributed = new QueryEngineDistributed(spark, 1);
        final Statement importStmt;
        try {
            importStmt = parser.createStatement(importStr.replace(";", ""));
            assertTrue( "ingestQuery should generate a Statement of type Query",
                    importStmt instanceof ImportCsv);
        } catch (ParsingException e) {
            e.printStackTrace();
            throw new Error("ingestQuery should parse");
        }
        final ImportCsv importStatement = (ImportCsv) importStmt;
        queryEngineDistributed.importTableFromCsv(importStatement);

        final String diffStr = "SELECT * FROM DIFF (SELECT * FROM sample WHERE version=\"v1\") outliers, (SELECT * FROM sample WHERE version=\"v2\") inliers ON location WITH MIN RATIO 1.2 MIN SUPPORT 0.05;";

        final Statement diffStmt;
        try {
            diffStmt = parser.createStatement(diffStr.replace(";", ""));
        } catch (ParsingException e) {
            e.printStackTrace();
            throw new Error("ingestQuery should parse");
        }
        final Query diffQuery = (Query) diffStmt;
        Dataset<Row> diffResult = queryEngineDistributed.executeQuery(diffQuery);

        List<Row> collectedResult = diffResult.collectAsList();

        assertEquals(1, collectedResult.size());

        assertEquals("USA", collectedResult.get(0).getString(0));
        assertEquals(0.7168, collectedResult.get(0).getDouble(1), 0.01);
        assertEquals(1.9247, collectedResult.get(0).getDouble(2), 0.01);
        assertEquals(200.0, collectedResult.get(0).getDouble(3));
        assertEquals(200.0, collectedResult.get(0).getDouble(4));

        spark.stop();
    }

    @Test
    public void testDiffSplit() throws Exception {
        final String importStr = "IMPORT FROM CSV FILE \"../core/demo/sample.csv\" INTO sample(usage string, latency string, location string, version string);";
        SqlParser parser = new SqlParser();
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("macrobase-sql-spark")
                .getOrCreate();
        QueryEngineDistributed queryEngineDistributed = new QueryEngineDistributed(spark, 1);
        final Statement importStmt;
        try {
            importStmt = parser.createStatement(importStr.replace(";", ""));
            assertTrue( "ingestQuery should generate a Statement of type Query",
                    importStmt instanceof ImportCsv);
        } catch (ParsingException e) {
            e.printStackTrace();
            throw new Error("ingestQuery should parse");
        }
        final ImportCsv importStatement = (ImportCsv) importStmt;
        queryEngineDistributed.importTableFromCsv(importStatement);

        final String diffStr = "SELECT * FROM DIFF (SPLIT sample WHERE version=\"v1\") ON location WITH MIN RATIO 1.2 MIN SUPPORT 0.05;";

        final Statement diffStmt;
        try {
            diffStmt = parser.createStatement(diffStr.replace(";", ""));
        } catch (ParsingException e) {
            e.printStackTrace();
            throw new Error("ingestQuery should parse");
        }
        final Query diffQuery = (Query) diffStmt;
        Dataset<Row> diffResult = queryEngineDistributed.executeQuery(diffQuery);

        List<Row> collectedResult = diffResult.collectAsList();

        assertEquals(1, collectedResult.size());

        assertEquals("USA", collectedResult.get(0).getString(0));
        assertEquals(0.7168, collectedResult.get(0).getDouble(1), 0.01);
        assertEquals(3.7885, collectedResult.get(0).getDouble(2), 0.01);
        assertEquals(200.0, collectedResult.get(0).getDouble(3));
        assertEquals(200.0, collectedResult.get(0).getDouble(4));

        spark.stop();
    }

}

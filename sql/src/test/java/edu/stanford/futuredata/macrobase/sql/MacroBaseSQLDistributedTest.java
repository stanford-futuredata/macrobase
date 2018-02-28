package edu.stanford.futuredata.macrobase.sql;

import edu.stanford.futuredata.macrobase.sql.parser.ParsingException;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
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
        QueryEngineDistributed queryEngineDistributed = new QueryEngineDistributed(spark);
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

}

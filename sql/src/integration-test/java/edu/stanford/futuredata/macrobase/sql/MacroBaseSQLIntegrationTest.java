package edu.stanford.futuredata.macrobase.sql;

import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.sql.parser.ParsingException;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.QueryBody;
import edu.stanford.futuredata.macrobase.sql.tree.Statement;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import edu.stanford.futuredata.macrobase.util.MacrobaseSQLException;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class MacroBaseSQLIntegrationTest {

    private SqlParser parser;
    private QueryEngine queryEngine;
    private DataFrame input;


    public MacroBaseSQLIntegrationTest() {
    }

    @Before
    public void setUp() throws Exception {
        input = new CSVDataFrameParser("../core/demo/sample.csv",
            Arrays.asList("usage", "latency", "location", "version"))
            .setColumnTypes(ImmutableMap.of("usage", ColType.DOUBLE, "latency", ColType.DOUBLE))
            .load();
        queryEngine = new QueryEngine();
        parser = new SqlParser();

        final String importQueryStr = Resources
            .toString(Resources.getResource("import.sql"), Charsets.UTF_8);
        System.out.println(importQueryStr);
        final Statement stmt;
        try {
            stmt = parser.createStatement(importQueryStr.replace(";", ""));
            assertTrue("import.sql should generate a Statement of type ImportCsv",
                stmt instanceof ImportCsv);
        } catch (ParsingException e) {
            e.printStackTrace();
            throw new Error("import.sql should parse");
        }

        final ImportCsv importStatement = (ImportCsv) stmt;
        try {
            final DataFrame df = queryEngine.importTableFromCsv(importStatement);
            assertTrue(df.equals(input));
        } catch (MacrobaseSQLException e) {
            e.printStackTrace();
            throw new Error("import.sql should not throw an exception");
        }
    }

    @Test
    public void queryOne() throws IOException {
        runQueryFromFile("one.sql", new DataFrame());
    }

    @Test
    public void queryTwo() throws IOException {
        runQueryFromFile("two.sql", new DataFrame());
    }

    @Test
    public void testAllQueries() throws IOException {
        queryOne();
        queryTwo();
    }

    // TODO: Add DF argument to check against
    private void runQueryFromFile(final String queryFilename, final DataFrame expected)
        throws IOException {
        final String queryStr = Resources
            .toString(Resources.getResource(queryFilename), Charsets.UTF_8);
        final Statement stmt;
        try {
            stmt = parser.createStatement(queryStr.replace(";", ""));
            assertTrue(queryFilename + " should generate a Statement of type Query",
                stmt instanceof Query);
        } catch (ParsingException e) {
            e.printStackTrace();
            throw new Error(queryFilename + " should parse");
        }
        final QueryBody q = ((Query) stmt).getQueryBody();
        try {
            final DataFrame result = queryEngine.executeQuery(q);
            assertTrue(expected.equals(result));
        } catch (MacrobaseException e) {
            e.printStackTrace();
            throw new Error(queryFilename + " should not throw an exception");
        }
    }

}
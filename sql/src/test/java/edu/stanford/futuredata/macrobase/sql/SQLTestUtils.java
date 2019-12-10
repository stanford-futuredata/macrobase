package edu.stanford.futuredata.macrobase.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
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
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

class SQLTestUtils {

    static void runQueryFromFile(final SqlParser parser, final QueryEngine queryEngine,
        final String queryFilename, final DataFrame expected)
        throws IOException {
        final String queryStr = Resources
            .toString(Resources.getResource(queryFilename), Charsets.UTF_8);
        System.out.println(queryStr);
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
            result.prettyPrint();
            assertEquals(expected, result);
        } catch (MacroBaseException e) {
            e.printStackTrace();
            throw new Error(queryFilename + " should not throw an exception");
        }
    }

    static void runQueriesFromFile(final SqlParser parser, final QueryEngine queryEngine,
        final String queryFilename, final DataFrame expected)
        throws IOException {
        final List<String> queriesInFile = Resources
            .readLines(Resources.getResource(queryFilename), Charsets.UTF_8);

        for (String queryStr : queriesInFile) {
            if (queryStr.isEmpty()) {
                continue;
            }
            final Statement stmt;
            try {
                stmt = parser.createStatement(queryStr.replace(";", ""));
                if (queryStr.startsWith("SELECT")) {
                    assertTrue(queryStr + " should generate a Statement of type Query",
                        stmt instanceof Query);
                } else if (queryStr.startsWith("IMPORT")) {
                    assertTrue(queryStr + " should generate a Statement of type ImportCsv",
                        stmt instanceof ImportCsv);
                }
                if (stmt instanceof Query) {
                    final QueryBody q = ((Query) stmt).getQueryBody();
                    final DataFrame result = queryEngine.executeQuery(q);
                    result.prettyPrint();
                    assertEquals(expected, result);
                } else if (stmt instanceof ImportCsv) {
                    final ImportCsv importStatement = (ImportCsv) stmt;
                    queryEngine.importTableFromCsv(importStatement);
                }
            } catch (MacroBaseException e) {
                e.printStackTrace();
                throw new Error(queryFilename + " should not throw an exception");
            } catch (ParsingException e) {
                e.printStackTrace();
                throw new Error(queryFilename + " should parse");
            }
        }
    }

    static DataFrame loadDataFrameFromCsv(final String csvFilename,
        final Map<String, ColType> schema) throws Exception {
        return new CSVDataFrameParser(Resources.getResource(csvFilename).getFile(), schema, false).load();
    }

}

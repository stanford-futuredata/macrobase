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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MacroBaseSQLJoinTest {

    private SqlParser parser;
    private QueryEngine queryEngine;

    @Before
    public void setUp() throws Exception {
        queryEngine = new QueryEngine();
        parser = new SqlParser();
    }

    @Test
    public void joinQuery1() throws Exception {
        runQueriesFromFile("joins/1.sql",
            loadDataFrameFromCSV("joins/1.csv", ImmutableMap.of("A0", ColType.STRING)));
    }

    @Test
    public void joinQuery2() throws Exception {
        runQueriesFromFile("joins/2.sql",
            loadDataFrameFromCSV("joins/2.csv", ImmutableMap
                .of("a.A0", ColType.STRING, "A1", ColType.STRING, "b.A2", ColType.STRING)));
    }

    @Test
    public void joinQuery3() throws Exception {
        runQueriesFromFile("joins/3.sql",
            loadDataFrameFromCSV("joins/3.csv", ImmutableMap
                .of("a.A0", ColType.STRING, "A1", ColType.STRING, "a.A2", ColType.STRING, "b.A2",
                    ColType.STRING, "b.A3", ColType.STRING)));
    }

    @Test
    public void joinQuery4() throws Exception {
        runQueriesFromFile("joins/4.sql",
            loadDataFrameFromCSV("joins/4.csv", ImmutableMap
                .of("a.A0", ColType.STRING, "A1", ColType.STRING, "b.A2", ColType.STRING)));
    }

    private DataFrame loadDataFrameFromCSV(final String csvFilename,
        final Map<String, ColType> schema) throws Exception {
        return new CSVDataFrameParser(Resources.getResource(csvFilename).getFile(), schema).load();
    }

    private void runQueriesFromFile(final String queryFilename, final DataFrame expected)
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
                    assertTrue(expected.equals(result));
                } else if (stmt instanceof ImportCsv) {
                    final ImportCsv importStatement = (ImportCsv) stmt;
                    queryEngine.importTableFromCsv(importStatement);
                }
            } catch (MacrobaseException e) {
                e.printStackTrace();
                throw new Error(queryFilename + " should not throw an exception");
            } catch (ParsingException e) {
                e.printStackTrace();
                throw new Error(queryFilename + " should parse");
            }
        }
    }
}

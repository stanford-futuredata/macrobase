package edu.stanford.futuredata.macrobase.sql;

import static edu.stanford.futuredata.macrobase.sql.SQLTestUtils.loadDataFrameFromCSV;
import static edu.stanford.futuredata.macrobase.sql.SQLTestUtils.runQueryFromFile;
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
import edu.stanford.futuredata.macrobase.sql.tree.Statement;
import edu.stanford.futuredata.macrobase.util.MacroBaseSQLException;
import java.util.Arrays;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MacroBaseSQLTest {

    private SqlParser parser;
    private QueryEngine queryEngine;

    private final Map<String, ColType> GLOBAL_RATIO_SCHEMA = ImmutableMap.<String, ColType>builder()
        .put("location", ColType.STRING)
        .put("version", ColType.STRING)
        .put("support", ColType.DOUBLE)
        .put("global_ratio", ColType.DOUBLE)
        .put("outlier_count", ColType.DOUBLE)
        .put("total_count", ColType.DOUBLE)
        .build();

    private final Map<String, ColType> RISK_RATIO_SCHEMA = ImmutableMap.<String, ColType>builder()
        .put("location", ColType.STRING)
        .put("version", ColType.STRING)
        .put("support", ColType.DOUBLE)
        .put("risk_ratio", ColType.DOUBLE)
        .put("outlier_count", ColType.DOUBLE)
        .put("total_count", ColType.DOUBLE)
        .build();

    @Before
    // Load sample.csv into memory, so we can use it for all subsequent tests
    public void setUp() throws Exception {
        final DataFrame input = new CSVDataFrameParser("../core/demo/sample.csv",
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
        } catch (MacroBaseSQLException e) {
            e.printStackTrace();
            throw new Error("import.sql should not throw an exception");
        }
    }

    @Test
    public void query1() throws Exception {
        runQueryFromFile(parser, queryEngine, "1.sql",
            loadDataFrameFromCSV("1.csv", GLOBAL_RATIO_SCHEMA));
    }

    @Test
    public void query2() throws Exception {
        runQueryFromFile(parser, queryEngine, "2.sql",
            loadDataFrameFromCSV("2.csv", GLOBAL_RATIO_SCHEMA));
    }

    @Test
    public void query3() throws Exception {
        runQueryFromFile(parser, queryEngine, "3.sql",
            loadDataFrameFromCSV("3.csv", GLOBAL_RATIO_SCHEMA));
    }

    @Test
    public void query4() throws Exception {
        runQueryFromFile(parser, queryEngine, "4.sql",
            loadDataFrameFromCSV("4.csv", GLOBAL_RATIO_SCHEMA));
    }

    @Test
    public void query5() throws Exception {
        runQueryFromFile(parser, queryEngine, "5.sql",
            loadDataFrameFromCSV("5.csv", GLOBAL_RATIO_SCHEMA));
    }

    @Test
    public void query6() throws Exception {
        runQueryFromFile(parser, queryEngine, "6.sql",
            loadDataFrameFromCSV("6.csv", RISK_RATIO_SCHEMA));
    }

    @Test
    public void query7() throws Exception {
        runQueryFromFile(parser, queryEngine, "7.sql",
            loadDataFrameFromCSV("7.csv", GLOBAL_RATIO_SCHEMA));
    }

    @Test
    public void query8() throws Exception {
        runQueryFromFile(parser, queryEngine, "8.sql",
            loadDataFrameFromCSV("8.csv", RISK_RATIO_SCHEMA));
    }

    @Test
    public void query9() throws Exception {
        runQueryFromFile(parser, queryEngine, "9.sql",
            loadDataFrameFromCSV("9.csv", RISK_RATIO_SCHEMA));
    }

    @Test
    public void query10() throws Exception {
        runQueryFromFile(parser, queryEngine, "10.sql",
            loadDataFrameFromCSV("10.csv", RISK_RATIO_SCHEMA));
    }

    @Test
    public void query11() throws Exception {
        runQueryFromFile(parser, queryEngine, "11.sql",
            loadDataFrameFromCSV("11.csv", RISK_RATIO_SCHEMA));
    }

    @Test
    public void query12() throws Exception {
        runQueryFromFile(parser, queryEngine, "12.sql",
            loadDataFrameFromCSV("12.csv", RISK_RATIO_SCHEMA));
    }

    @Test
    public void query13() throws Exception {
        runQueryFromFile(parser, queryEngine, "13.sql",
            loadDataFrameFromCSV("13.csv", ImmutableMap.of("percentile(usage)", ColType.DOUBLE)));
    }

    @Test
    public void query14() throws Exception {
        runQueryFromFile(parser, queryEngine, "14.sql",
            loadDataFrameFromCSV("14.csv", ImmutableMap.of("pct", ColType.DOUBLE)));
    }

    @Test
    public void query15() throws Exception {
        runQueryFromFile(parser, queryEngine, "15.sql", loadDataFrameFromCSV("15.csv",
            ImmutableMap.of("usage", ColType.DOUBLE, "pct", ColType.DOUBLE)));
    }

    @Test
    public void query16() throws Exception {
        runQueryFromFile(parser, queryEngine, "16.sql", loadDataFrameFromCSV("16.csv", ImmutableMap
            .of("usage", ColType.DOUBLE, "latency", ColType.DOUBLE, "location", ColType.STRING,
                "version", ColType.STRING, "pct", ColType.DOUBLE)));
    }

    @Test
    public void query17() throws Exception {
        runQueryFromFile(parser, queryEngine, "17.sql",
            loadDataFrameFromCSV("17.csv", ImmutableMap.of("pct", ColType.DOUBLE)));
    }

    @Test
    public void query18() throws Exception {
        runQueryFromFile(parser, queryEngine, "18.sql", loadDataFrameFromCSV("18.csv", ImmutableMap
            .of("usage", ColType.DOUBLE, "latency", ColType.DOUBLE, "location", ColType.STRING,
                "version", ColType.STRING, "pct", ColType.DOUBLE)));
    }

    @Test
    public void query19() throws Exception {
        runQueryFromFile(parser, queryEngine, "19.sql", loadDataFrameFromCSV("19.csv",
            ImmutableMap.of("usage", ColType.DOUBLE, "pct", ColType.DOUBLE)));
    }

    @Test
    public void query20() throws Exception {
        runQueryFromFile(parser, queryEngine, "20.sql",
            loadDataFrameFromCSV("20.csv", ImmutableMap.of("usage", ColType.DOUBLE)));
    }

    @Test
    public void testAllQueries() throws Exception {
        query1();
        query2();
        query3();
        query4();
        query5();
        query6();
        query7();
        query8();
        query9();
        query10();
        query11();
        query12();
        query13();
        query14();
        query15();
        query16();
        query17();
        query18();
        query19();
        query20();
    }
}
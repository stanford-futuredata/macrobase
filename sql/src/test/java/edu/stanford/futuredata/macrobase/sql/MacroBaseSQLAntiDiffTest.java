package edu.stanford.futuredata.macrobase.sql;

import static edu.stanford.futuredata.macrobase.sql.SQLTestUtils.loadDataFrameFromCsv;
import static edu.stanford.futuredata.macrobase.sql.SQLTestUtils.runQueriesFromFile;

import com.google.common.collect.ImmutableMap;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MacroBaseSQLAntiDiffTest {

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

    private final Map<String, ColType> QUERY_4_SCHEMA = ImmutableMap.<String, ColType>builder()
        .put("location", ColType.STRING)
        .put("version", ColType.STRING)
        .put("device", ColType.STRING)
        .put("support", ColType.DOUBLE)
        .put("global_ratio", ColType.DOUBLE)
        .put("outlier_count", ColType.DOUBLE)
        .put("total_count", ColType.DOUBLE)
        .build();

    @Before
    public void setUp() {
        queryEngine = new QueryEngine();
        parser = new SqlParser();
    }

    @Test
    public void query1() throws Exception {
        runQueriesFromFile(parser, queryEngine, "anti-diff/1.sql",
            loadDataFrameFromCsv("anti-diff/1.csv", GLOBAL_RATIO_SCHEMA));
    }

     @Test
     public void query2() throws Exception {
         runQueriesFromFile(parser, queryEngine, "anti-diff/2.sql",
             loadDataFrameFromCsv("anti-diff/2.csv", GLOBAL_RATIO_SCHEMA));
     }

    @Test
    public void query3() throws Exception {
        runQueriesFromFile(parser, queryEngine, "anti-diff/3.sql",
            loadDataFrameFromCsv("anti-diff/3.csv", GLOBAL_RATIO_SCHEMA));
    }


    @Test
    public void query4() throws Exception {
        runQueriesFromFile(parser, queryEngine, "anti-diff/4.sql",
            loadDataFrameFromCsv("anti-diff/4.csv", QUERY_4_SCHEMA));
    }

    @Test
    public void testAllQueries() throws Exception {
        query1();
        query2();
        query3();
        query4();
    }
}


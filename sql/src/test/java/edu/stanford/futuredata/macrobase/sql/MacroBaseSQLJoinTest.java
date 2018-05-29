package edu.stanford.futuredata.macrobase.sql;

import static edu.stanford.futuredata.macrobase.sql.SQLTestUtils.loadDataFrameFromCsv;
import static edu.stanford.futuredata.macrobase.sql.SQLTestUtils.runQueriesFromFile;

import com.google.common.collect.ImmutableMap;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
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
        runQueriesFromFile(parser, queryEngine, "joins/1.sql",
            loadDataFrameFromCsv("joins/1.csv", ImmutableMap.of("A0", ColType.STRING)));
    }

    @Test
    public void joinQuery2() throws Exception {
        runQueriesFromFile(parser, queryEngine, "joins/2.sql",
            loadDataFrameFromCsv("joins/2.csv", ImmutableMap
                .of("A0", ColType.STRING, "A1", ColType.STRING, "A2", ColType.STRING)));
    }

    @Test
    public void joinQuery3() throws Exception {
        runQueriesFromFile(parser, queryEngine, "joins/3.sql",
            loadDataFrameFromCsv("joins/3.csv", ImmutableMap
                .of("A0", ColType.STRING, "A1", ColType.STRING, "a.A2", ColType.STRING, "b.A2",
                    ColType.STRING, "A3", ColType.STRING)));
    }

    @Test
    public void joinQuery4() throws Exception {
        runQueriesFromFile(parser, queryEngine, "joins/4.sql",
            loadDataFrameFromCsv("joins/4.csv", ImmutableMap
                .of("A0", ColType.STRING, "A1", ColType.STRING, "A2", ColType.STRING)));
    }
}

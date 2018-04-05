package edu.stanford.futuredata.macrobase.sql;

import static edu.stanford.futuredata.macrobase.sql.SQLTestUtils.loadDataFrameFromCSV;
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
            loadDataFrameFromCSV("joins/1.csv", ImmutableMap.of("A0", ColType.STRING)));
    }

    @Test
    public void joinQuery2() throws Exception {
        runQueriesFromFile(parser, queryEngine, "joins/2.sql",
            loadDataFrameFromCSV("joins/2.csv", ImmutableMap
                .of("A0", ColType.STRING, "A1", ColType.STRING, "A2", ColType.STRING)));
    }

    @Test
    public void joinQuery3() throws Exception {
        runQueriesFromFile(parser, queryEngine, "joins/3.sql",
            loadDataFrameFromCSV("joins/3.csv", ImmutableMap
                .of("A0", ColType.STRING, "A1", ColType.STRING, "a.A2", ColType.STRING, "b.A2",
                    ColType.STRING, "A3", ColType.STRING)));
    }

    @Test
    public void joinQuery4() throws Exception {
        runQueriesFromFile(parser, queryEngine, "joins/4.sql",
            loadDataFrameFromCSV("joins/4.csv", ImmutableMap
                .of("A0", ColType.STRING, "A1", ColType.STRING, "A2", ColType.STRING)));
    }
}

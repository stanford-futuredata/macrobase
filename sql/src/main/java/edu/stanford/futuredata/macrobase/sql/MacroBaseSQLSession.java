package edu.stanford.futuredata.macrobase.sql;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.QueryBody;
import edu.stanford.futuredata.macrobase.sql.tree.Statement;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseSQLSession {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseSQLSession.class);

    private final SqlParser parser;
    private final QueryEngine queryEngine;

    public MacroBaseSQLSession() {
        parser = new SqlParser();
        queryEngine = new QueryEngine();
    }

    public DataFrame executeQuery(final String queryStr) throws MacroBaseException {
        Statement stmt = parser.createStatement(queryStr);
        log.debug(stmt.toString());
        if (stmt instanceof ImportCsv) {
            final ImportCsv importStatement = (ImportCsv) stmt;
            return queryEngine.importTableFromCsv(importStatement);
        } else {
            final QueryBody q = ((Query) stmt).getQueryBody();
            return queryEngine.executeQuery(q);
        }
    }
}

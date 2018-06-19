package edu.stanford.futuredata.macrobase.sql;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.QueryBody;
import edu.stanford.futuredata.macrobase.sql.tree.Statement;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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
            DataFrame df = queryEngine.importTableFromCsv(importStatement);
            return getImportInfo(df);
        } else {
            final QueryBody q = ((Query) stmt).getQueryBody();
            return queryEngine.executeQuery(q);
        }
    }

    private static DataFrame getImportInfo(final DataFrame data) {
        Schema schema = new Schema();
        schema.addColumn(Schema.ColType.STRING, "columnName");
        schema.addColumn(Schema.ColType.STRING, "type");

        Schema oldSchema = data.getSchema();
        int numColumns = 2;
        int numRows = oldSchema.getNumColumns();
        ArrayList<Row> rows = new ArrayList<Row>(numRows);

        for (int i = 0; i < numRows; i++) {
            ArrayList<Object> vals = new ArrayList<Object>(numColumns);
            vals.add(oldSchema.getColumnName(i));
            if(oldSchema.getColumnType(i) == Schema.ColType.STRING) {
                vals.add("attribute");
            } else if (oldSchema.getColumnType(i) == Schema.ColType.DOUBLE) {
                vals.add("metric");
            } else {
                vals.add("none");
            }

            Row row = new Row(vals);
            rows.add(row);
        }

        DataFrame df = new DataFrame(schema, rows);
        return df;
    }
}

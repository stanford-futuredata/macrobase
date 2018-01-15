package edu.stanford.futuredata.macrobase.sql;

import static java.nio.file.Files.exists;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameWriter;
import edu.stanford.futuredata.macrobase.sql.parser.ParsingException;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import edu.stanford.futuredata.macrobase.sql.parser.StatementSplitter;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.QueryBody;
import edu.stanford.futuredata.macrobase.sql.tree.Statement;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import jline.console.ConsoleReader;
import jline.console.completer.CandidateListCompletionHandler;
import jline.console.completer.FileNameCompleter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseSQLRepl {

    private static final String ASCII_ART_FILE = "ascii_art.txt";
    private static final Logger log = LoggerFactory.getLogger(MacroBaseSQLRepl.class);

    private final ConsoleReader reader;
    private final SqlParser parser;
    private final QueryEngine queryEngine;

    /**
     * Main entry point to the SQL CLI interface in MacroBase
     *
     * @throws IOException if unable to instantiate ConsoleReader
     */
    private MacroBaseSQLRepl() throws IOException {
        // Initialize console reader and writer
        reader = new ConsoleReader();
        final CandidateListCompletionHandler handler = new CandidateListCompletionHandler();
        handler.setStripAnsi(true);
        reader.setCompletionHandler(handler);
        reader.addCompleter(new FileNameCompleter());

        parser = new SqlParser();
        queryEngine = new QueryEngine();
    }

    /**
     * Executes one or more SQL queries.
     *
     * @param queries A single String which contains the queries to execute. Each query in the
     * String should be delimited by ';' and optional whitespace.
     * @param print If True, print query to the console (useful when reading queries from file)
     */
    private void executeQueries(final String queries, final boolean print) {
        StatementSplitter splitter = new StatementSplitter(queries);
        for (StatementSplitter.Statement s : splitter.getCompleteStatements()) {
            final String statementStr = s.statement();
            if (print) {
                System.out.println(statementStr + ";");
                System.out.println();
                System.out.flush();
            }
            // Remove extra whitespace and add delimiter before updating console history
            reader.getHistory().add(statementStr.replaceAll("\\s+", " ") + ";");
            try {
                Statement stmt = parser.createStatement(statementStr);
                log.debug(stmt.toString());
                if (stmt instanceof ImportCsv) {
                    final ImportCsv importStatement = (ImportCsv) stmt;
                    queryEngine.importTableFromCsv(importStatement).prettyPrint();
                } else {
                    QueryBody q = ((Query) stmt).getQueryBody();
                    final DataFrame result = queryEngine.executeQuery(q);
                    result.prettyPrint();
                    q.getExportExpr().ifPresent((exportExpr) -> {
                        // print result to file; if file already exists, do nothing and print error message
                        final String filename = exportExpr.getFilename();
                        if (!exists(Paths.get(filename))) {
                            try (OutputStreamWriter outFile = new OutputStreamWriter(
                                new FileOutputStream(filename))) {
                                new CSVDataFrameWriter(exportExpr.getFieldDelimiter(),
                                    exportExpr.getLineDelimiter()).writeToStream(result, outFile);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.err.println("File " + filename + " already exists.");
                            System.err.println();
                        }
                    });
                }
            } catch (ParsingException | MacrobaseException e) {
                System.err.println(e.getMessage());
                System.err.println();
            }
        }
    }

    /**
     * Executes one or more SQL queries. Calls {@link #executeQueries(String, boolean)} with 2nd
     * argument set to false.
     *
     * @param queries A single String which contains the queries to execute. Each query in the
     * String should be delimited by ';' and optional whitespace.
     */

    private void executeQueries(final String queries) {
        executeQueries(queries, false);
    }

    /**
     * Start the indefinite Read-Eval-Print loop for executing SQL queries.
     *
     * @throws IOException if there as an issue reading input from the command line
     */
    private void runRepl() throws IOException {
        while (true) {
            final String query;
            query = readConsoleInput();
            if (query.equals("")) {
                break;
            }

            executeQueries(query);
        }
    }

    private String readConsoleInput() throws IOException {
        reader.setPrompt("macrobase-sql> ");
        String line = reader.readLine();
        if (line == null || line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
            return "";
        }
        if (line.endsWith(";")) {
            return line;
        }
        reader.setPrompt("... ");
        StringBuilder commandBuilder = new StringBuilder(line);
        while (!line.endsWith(";")) {
            line = reader.readLine();
            if (line == null) {
                return "";
            }
            commandBuilder.append("\n");
            commandBuilder.append(line);
        }
        reader.setPrompt("macrobase-sql> ");
        return commandBuilder.toString();
    }

    public static void main(String... args) throws IOException {
        final MacroBaseSQLRepl repl = new MacroBaseSQLRepl();
        final String asciiArt = Resources
            .toString(Resources.getResource(ASCII_ART_FILE), Charsets.UTF_8);
        boolean printedWelcome = false;
        if (args != null && args.length > 0) {
            switch (args[0].toLowerCase()) {
                case "-h":
                case "--help":
                    usage();
                    return;
                case "-f":
                case "--file":
                    if (args.length > 1) {
                        System.out.println(asciiArt);
                        printedWelcome = true;
                        final String queriesFromFile = Files
                            .toString(new File(args[1]), Charsets.UTF_8);
                        repl.executeQueries(queriesFromFile, true);
                    }
                    break;
            }
        }

        if (!printedWelcome) {
            System.out.println(asciiArt);
        }

        repl.runRepl();
    }

    /**
     * Print MacroBase SQL usage.
     */
    private static void usage() {
        System.out.println("Usage: java " + MacroBaseSQLRepl.class.getName());
    }

}

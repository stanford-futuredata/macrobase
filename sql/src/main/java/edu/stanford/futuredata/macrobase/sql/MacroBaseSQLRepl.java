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
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.file.Paths;
import jline.console.ConsoleReader;
import jline.console.completer.CandidateListCompletionHandler;
import jline.console.completer.FileNameCompleter;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacroBaseSQLRepl {

    private static final String ASCII_ART_FILE = "ascii_art.txt";
    private static final Logger log = LoggerFactory.getLogger(MacroBaseSQLRepl.class);

    private final ConsoleReader reader;
    private final SqlParser parser;
    private final QueryEngine queryEngine;
    private final boolean paging;

    private File tempFileForPaging;

    /**
     * Main entry point to the SQL CLI interface in MacroBase
     *
     * @param userWantsPaging try to enable paging of results in SQL shell
     * @throws IOException if unable to instantiate ConsoleReader
     */
    private MacroBaseSQLRepl(final boolean userWantsPaging) throws IOException {
        // First try to turn paging on
        this.paging = enablePaging(userWantsPaging);
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
     * Try to turn on paging of results in SQL shell, if enabled by user.
     * @param userWantsPaging if user set --paging in the command line
     * @return if userWantsPaging is false, return false. Otherwise, try to create temp file
     * for paging and reading it using <code>less</code>. If an exception is thrown (i.e., either
     * creating or reading the file failed)
     */
    private boolean enablePaging(boolean userWantsPaging) {
        if (!userWantsPaging) {
            return false;
        }
        try {
            tempFileForPaging = File.createTempFile("mb-sql", null);
            final Process p = Runtime.getRuntime()
                .exec(new String[]{"less", tempFileForPaging.getAbsolutePath()});
            return p.waitFor() == 0;
        } catch (IOException | InterruptedException e) {
            log.warn("--paging set to true, but unable to enable paging");
            return false;
        }
    }

    /**
     * Executes one or more SQL queries.
     *
     * @param queries A single String which contains the queries to execute. Each query in the
     * String should be delimited by ';' and optional whitespace.
     * @param fromFile If True, queries have been read from File
     */
    private void executeQueries(final String queries, final boolean fromFile) {
        StatementSplitter splitter = new StatementSplitter(queries);
        for (StatementSplitter.Statement s : splitter.getCompleteStatements()) {
            final String statementStr = s.statement();
            if (fromFile) {
                System.out.println(statementStr + ";");
                System.out.println();
                System.out.flush();
            }
            // Remove extra whitespace and add delimiter before updating console history
            reader.getHistory().add(statementStr.replaceAll("\\s+", " ") + ";");
            try {
                Statement stmt = parser.createStatement(statementStr);
                log.debug(stmt.toString());
                final DataFrame result;
                if (stmt instanceof ImportCsv) {
                    final ImportCsv importStatement = (ImportCsv) stmt;
                    result = queryEngine.importTableFromCsv(importStatement);
                } else {
                    final QueryBody q = ((Query) stmt).getQueryBody();
                    result = queryEngine.executeQuery(q);
                }
                if (paging) {
                    try {
                        final PrintStream ps = new PrintStream(
                            new FileOutputStream(tempFileForPaging.getAbsolutePath()));
                        result.prettyPrint(ps, -1);
                        ProcessBuilder pb = new ProcessBuilder("less",
                            tempFileForPaging.getAbsolutePath());
                        pb.inheritIO();
                        Process p = pb.start();
                        p.waitFor();
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    result.prettyPrint();
                }
                if (stmt instanceof Query) {
                    final QueryBody q = ((Query) stmt).getQueryBody();
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
            } catch (ParsingException | MacroBaseException e) {
                e.printStackTrace(System.err);
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
        ArgumentParser parser = ArgumentParsers.newFor("MacroBase SQL").build()
            .defaultHelp(true)
            .description("Run MacroBase SQL.");
        parser.addArgument("-f", "--file").help("Load file with SQL queries to execute");
        parser.addArgument("-p", "--paging").type(Arguments.booleanType()).setDefault(false)
            .help("Turn on paging of results for SQL queries");
        final Namespace parsedArgs = parser.parseArgsOrFail(args);

        final MacroBaseSQLRepl repl = new MacroBaseSQLRepl(parsedArgs.get("paging"));
        final String asciiArt = Resources
            .toString(Resources.getResource(ASCII_ART_FILE), Charsets.UTF_8);

        boolean printedWelcome = false;
        if (parsedArgs.get("file") != null) {
            System.out.println(asciiArt);
            printedWelcome = true;
            final String queriesFromFile = Files
                .toString(new File((String) parsedArgs.get("file")), Charsets.UTF_8);
            repl.executeQueries(queriesFromFile, true);
        }
        if (!printedWelcome) {
            System.out.println(asciiArt);
        }
        repl.runRepl();
    }
}

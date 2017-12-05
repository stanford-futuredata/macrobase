package edu.stanford.futuredata.macrobase.sql;

import edu.stanford.futuredata.macrobase.sql.parser.ParsingException;
import edu.stanford.futuredata.macrobase.sql.parser.SqlParser;
import edu.stanford.futuredata.macrobase.sql.parser.StatementSplitter;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.Statement;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;
import jline.console.ConsoleReader;
import jline.console.completer.CandidateListCompletionHandler;
import jline.console.completer.FileNameCompleter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MacrobaseSQLRepl {

  private static final String ASCII_ART_FILE = "ascii_art.txt";
  private static final Logger log = LoggerFactory.getLogger(MacrobaseSQLRepl.class);

  private final ConsoleReader reader;
  private final PrintWriter out;
  private final SqlParser parser;
  private final QueryEngine queryEngine;

  /**
   * Main entry point to the SQL CLI interface in MacroBase
   *
   * @throws IOException if unable to instantiate ConsoleReader
   */
  private MacrobaseSQLRepl() throws IOException {
    // Initialize console reader and writer
    reader = new ConsoleReader();
    final CandidateListCompletionHandler handler = new CandidateListCompletionHandler();
    handler.setStripAnsi(true);
    reader.setCompletionHandler(handler);
    reader.addCompleter(new FileNameCompleter());
    out = new PrintWriter(reader.getOutput());

    parser = new SqlParser();
    queryEngine = new QueryEngine();
  }

  /**
   * Executes one or more SQL queries.
   *
   * @param queries A single String which contains the queries to execute. Each query in the String
   * should be delimited by ';' and optional whitespace.
   * @param print If True, print query to the console (useful when reading queries from file)
   */
  private void executeQueries(final String queries, final boolean print) {
    StatementSplitter splitter = new StatementSplitter(queries);
    for (StatementSplitter.Statement s : splitter.getCompleteStatements()) {
      final String statementStr = s.statement();
      if (print) {
        out.println(statementStr + ";");
        out.println();
        out.flush();
      }
      // Remove newlines and add delimiter when updating console history
      reader.getHistory().add(statementStr.replace('\n', ' ') + ";");
      try {
        Statement stmt = parser.createStatement(statementStr);
        log.debug(stmt.toString());
        if (stmt instanceof ImportCsv) {
          ImportCsv importStatement = (ImportCsv) stmt;
          queryEngine.importTableFromCsv(importStatement).prettyPrint();
        } else {
          Query q = (Query) stmt;
          queryEngine.executeQuery(q).prettyPrint();
        }
      } catch (ParsingException | MacrobaseException e) {
        System.err.println(e.getMessage());
      }
    }
  }

  /**
   * Executes one or more SQL queries. Calls {@link #executeQueries(String, boolean)} with 2nd
   * argument set to false.
   *
   * @param queries A single String which contains the queries to execute. Each query in the String
   * should be delimited by ';' and optional whitespace.
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
    reader.setPrompt("macrodiff> ");
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
    reader.setPrompt("macrodiff> ");
    return commandBuilder.toString();
  }

  public static void main(String... args) throws IOException {
    final String ascii_art = readResourcesFile(ASCII_ART_FILE);
    System.out.println(ascii_art);

    final MacrobaseSQLRepl repl = new MacrobaseSQLRepl();

    if (args != null && args.length > 0) {
      switch (args[0].toLowerCase()) {
        case "-h":
        case "--help":
          usage();
          return;
        case "-f":
        case "--file":
          if (args.length > 1) {
            final String queriesFromFile = readFile(new File(args[1]));
            repl.executeQueries(queriesFromFile, true);
          }
          break;
      }
    }

    repl.runRepl();
  }

  /**
   * Read file from resources folder and return file contents as a single String
   */
  private static String readResourcesFile(final String filename) {
    return readFile(
        new File(MacrobaseSQLRepl.class.getClassLoader().getResource(filename).getFile()));
  }

  /**
   * Read file and return file contents as a single String
   */
  private static String readFile(File file) {
    StringBuilder result = new StringBuilder("");
    try (Scanner scanner = new Scanner(file)) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        result.append(line).append("\n");
      }
      scanner.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result.toString();
  }

  /**
   * Print MacroBase SQL usage.
   */
  private static void usage() {
    System.out.println("Usage: java " + MacrobaseSQLRepl.class.getName());
  }
}

package edu.stanford.futuredata.macrobase.datamodel;

import static java.util.stream.Collectors.toList;

import com.google.common.base.Joiner;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

/**
 * Format for import / export small batches
 */
public class Row {

    // Formatter for printing out doubles; print at least 1 and no more than 6 decimal places
    static final DecimalFormat DOUBLE_FORMAT = new DecimalFormat("#.0#####",
        DecimalFormatSymbols.getInstance(Locale.US));

    private Schema schema; // not set by user
    private List<Object> vals;

    public Row(Schema schema, List<Object> vals) {
        this.schema = schema;
        this.vals = vals;
    }

    public Row(List<Object> vals) {
        this.schema = null;
        this.vals = vals;
    }

    public List<Object> getVals() {
        return this.vals;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAs(int i) {
        return (T) vals.get(i);
    }

    @SuppressWarnings("unchecked")
    public <T> T getAs(String colName) {
        if (schema == null) {
            throw new RuntimeException("No Schema");
        } else {
            return (T) vals.get(schema.getColumnIndex(colName));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Row row = (Row) o;
        return vals != null ? vals.equals(row.vals) : row.vals == null;
    }

    @Override
    public int hashCode() {
        return (vals != null ? vals.hashCode() : 0);
    }

    @Override
    public String toString() {
        return Joiner.on(",")
            .join(vals.stream().map(this::formatVal).collect(toList()));
    }

    /**
     * pretty print Row object to STDOUT or file (default: STDOUT), using a default width of 15
     * characters per value. Example output: |    val_1   |   val_2   | .... |   val_n   |
     */
    public void prettyPrint() {
        prettyPrint(System.out, 15);
    }

    /**
     * pretty print Row object to <tt>out</tt> using a default width of 15 characters per value.
     * Example output: |    val_1   |   val_2   | .... |   val_n   |
     */
    public void prettyPrint(final PrintStream out) {
        prettyPrint(out, 15);
    }

    /**
     * Pretty print Row object to STDOUT
     * Example output:
     * |    val_1   |   val_2   | .... |   val_n  |
     *
     * @param width number of characters to or each value, with <tt>(width - length of value) /
     * 2</tt> of whitespace on either side
     */
    public void prettyPrint(final int width) {
        prettyPrint(System.out, width);
    }

    /**
     * Pretty print Row object to the console.
     * Example output:
     * |    val_1   |   val_2   | .... | val_n   |
     *
     * @param out PrintStream to print Row to STDOUT or file (default: STDOUT)
     * @param width the number of characters to use for centering a single value. Increasing
     * <tt>width</tt> will increase the whitespace padding around each value.
     */
    public void prettyPrint(final PrintStream out, final int width) {
        out.println("|" + Joiner.on("|")
            .join(vals.stream().map((x) -> StringUtils.center(formatVal(x, width), width))
                .collect(toList())) + "|");
    }

    /**
     * Pretty print row in vertical, column-wise format. Example output:
     *
     * col_1    |  val_1
     * col_2    |  val_2
     * ...
     * col_n    |  val_n
     * -----------------------
     *
     * @param out PrintStream to print Row to STDOUT or file (default: STDOUT)
     */
    void prettyPrintColumnWise(final PrintStream out) {
        final int maxColNameLength = schema.getColumnNames().stream()
            .reduce("", (x, y) -> x.length() > y.length() ? x : y).length();

        int maxLength = 0;
        for (int i = 0; i < schema.getNumColumns(); ++i) {
            final String colName = schema.getColumnName(i);
            final Object val = vals.get(i);
            final String strToPrint =
                // truncate Strings longer than 40 chars
                StringUtils.rightPad(colName, maxColNameLength) + "  |  " + formatVal(val, 40);
            if (strToPrint.length() > maxLength) {
                maxLength = strToPrint.length();
            }
            out.println(strToPrint);
        }
        // add 5 dashes to account for "  |  "
        final String dashes = Joiner.on("").join(Collections.nCopies(maxLength + 5, "-"));
        out.println(dashes);
    }

    /**
     * @return If x is a double, return back a formatted String that prints at least 1 and up to 6
     * decimal places of the double. If x is null, return "-". Otherwise, return x unchanged (i.e.
     * toString will be used), but truncate it to @param width, if greater than 0
     */
    private String formatVal(Object x, final int width) {
        if (x == null) {
            return "-";
        } else if (x instanceof Double) {
            if (Double.isNaN((Double) x)) {
                return "NaN";
            }
            return DOUBLE_FORMAT.format(x);
        } else {
            final String str = String.valueOf(x);
            if (width > 0 && str.length() > width) {
                return str.substring(0, width - 3) + "...";
            } else {
                return str;
            }
        }
    }

    /**
     * @return format value for pretty-printing using default width of 15
     */
    private String formatVal(Object x) {
        return formatVal(x, 15);
    }

}

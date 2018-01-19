package edu.stanford.futuredata.macrobase.analysis;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 * MBFunction defines an interface for UDF-style functions that can be applied to a column of a
 * DataFrame on a per-row basis, thus generating a new column as output. Two limitations for now: 1)
 * MBFunctions only take in a single column as an argument; multi-column UDFs are not supported. 2)
 * MBFunctions only operate on columns of type double.
 *
 * To implement a particular UDF (e.g., to normalize values in a column between 0 and 1), subclass
 * MBFunction and implement {@link MBFunction#applyFunction(double[], double[])}. See {@link
 * NormalizeFunction} for an example.
 *
 * NOTE: All subclasses of MBFunction should include a construct that takes in a single String as
 * its sole argument; that String should be the column name that determines which column the
 * function will be applied on. (This is necessary for instantiating an MBFunction via the Java
 * Reflection API---all subclasses of MBFunction need to share the same set of arguments for
 * Exceptions to be avoided. If, in the future, MBFunction subclasses need additional arguments,
 * then a List-of-Strings approach will need to be used.)
 */
public abstract class MBFunction {

    protected final String columnName;

    /**
     * This constructor should be used by subclasses to properly initialize the column for the
     * function
     *
     * @param columnName The column to apply the function on
     */
    protected MBFunction(String columnName) {
        this.columnName = columnName;
    }

    /**
     * This method needs to be implemented in any subclass of MBFunction.
     *
     * @param inputCol The values of the
     */
    protected abstract void applyFunction(final double[] inputCol, final double[] outputCol);

    /**
     * Call this method to to invoke the function and generate the output column that results from
     * applying it on the DataFrame
     *
     * @throws MacrobaseException If the column name (specified in the constructor of the
     * MBFunction) isn't present in the DataFrame, an exception is thrown.
     */
    public final double[] apply(final DataFrame df) throws MacrobaseException {
        if (!df.hasColumn(columnName)) {
            throw new MacrobaseException(columnName + " not present in DataFrame");
        }
        final double[] outputCol = new double[df.getNumRows()];
        this.applyFunction(df.getDoubleColumnByName(columnName), outputCol);
        return outputCol;
    }

    /**
     * This method instantiates a given MBFunction given only the name of the function and the
     * argument for that function, which must be a single String (the column name). For example, to
     * instantiate a {@link NormalizeFunction}, on the column `attery_drain`, the {@param funcName}
     * would be "normalize"  (or "NORMALIZE"---the funcName is case-agnostic), and the {@param arg}
     * would be "battery_drain".
     *
     * @throws MacrobaseException If there's an error instantiating the MBFunction (usually due to
     * incorrect arguments or improperly defined subclasses), an exception is thrown.
     */
    public static MBFunction getFunction(String funcName, String arg)
        throws MacrobaseException {
        Class<? extends MBFunction> clazz;
        switch (funcName.toLowerCase()) {
            case "normalize": {
                clazz = NormalizeFunction.class;
                break;
            }
            default: {
                throw new MacrobaseException("Bad MBFunction Type: " + funcName);
            }
        }
        try {
            return clazz.getConstructor(String.class).newInstance(arg);
        } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new MacrobaseException(
                "MBFunction Type " + funcName + " incompatible with args (" + arg + ")");
        }
    }
}

/**
 * An MBFunction that normalizes all values in a column to be between 0 and 1.
 */
class NormalizeFunction extends MBFunction {

    /**
     * @param arg The column name
     */
    private NormalizeFunction(final String arg) {
        super(arg);
    }

    /**
     * @param inputCol The column values to normalized, remains unmodified
     * @param outputCol The new normalized values
     */
    @Override
    protected void applyFunction(final double[] inputCol, final double[] outputCol) {
        final double max = Arrays.stream(inputCol).max().getAsDouble();
        for (int i = 0; i < inputCol.length; ++i) {
            outputCol[i] = inputCol[i] / max;
        }
    }
}

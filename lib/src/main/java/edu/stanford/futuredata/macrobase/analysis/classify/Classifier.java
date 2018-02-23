package edu.stanford.futuredata.macrobase.analysis.classify;

import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import java.lang.reflect.InvocationTargetException;
import java.util.BitSet;
import java.util.List;

public abstract class Classifier implements Transformer {

    protected String columnName;
    protected String outputColumnName = "_OUTLIER";

    public Classifier(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public Classifier setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }

    /**
     * @param outputColumnName Which column to write the classification results.
     * @return this
     */
    public Classifier setOutputColumnName(String outputColumnName) {
        this.outputColumnName = outputColumnName;
        return this;
    }
}

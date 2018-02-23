package edu.stanford.futuredata.macrobase.distributed.analysis.classify;

import edu.stanford.futuredata.macrobase.distributed.operator.DistributedTransformer;

public abstract class DistributedClassifier implements DistributedTransformer {
    protected String columnName;
    protected String outputColumnName = "_OUTLIER";

    public DistributedClassifier(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }
    public DistributedClassifier setColumnName(String columnName) {
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
    public DistributedClassifier setOutputColumnName(String outputColumnName) {
        this.outputColumnName = outputColumnName;
        return this;
    }
}

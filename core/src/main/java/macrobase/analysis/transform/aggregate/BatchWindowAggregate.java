package macrobase.analysis.transform.aggregate;

public abstract class BatchWindowAggregate implements BatchAggregate {
    protected Integer timeColumn;
    protected int dim;
}

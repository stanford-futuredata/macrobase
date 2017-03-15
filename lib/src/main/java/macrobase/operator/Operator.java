package macrobase.operator;

public interface Operator<I,O> {
    void process(I input) throws Exception;
    O getResults();
}

package macrobase.operator;

public interface Operator<I,O> {
    Operator process(I input);
    O getResults();
}

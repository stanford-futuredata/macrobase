package macrobase.util;

import macrobase.datamodel.Datum;

import java.util.Iterator;

public class DiscardingIterator<T>  implements Iterator<T> {

    private final Iterator<T> input;

    public DiscardingIterator(Iterator<T> input, final int numToDiscard) {
        this.input = input;

        for(int i = 0; i < numToDiscard; ++i) {
            input.next();
        }
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public T next() {
        return input.next();
    }
}

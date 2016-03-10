package macrobase.util;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class IterUtils {
    public static <E> java.lang.Iterable<E> iterable(final Iterator<E> iterator) {
        if (iterator == null) {
            throw new NullPointerException();
        }
        return new Iterable<E>() {
            public Iterator<E> iterator() {
                return iterator;
            }
        };
    }

    public static <T> Stream<T> stream(Iterable<T> in) {
        return StreamSupport.stream(in.spliterator(), false);
    }

    public static <T> Stream<T> parallelStream(Iterable<T> in) {
        return StreamSupport.stream(in.spliterator(), true);
    }

    public static <T> Stream<T> stream(Iterator<T> in) {
        return stream(iterable(in));
    }

    public static <T> Stream<T> parallelStream(Iterator<T> in) {
        return parallelStream(iterable(in));
    }
}

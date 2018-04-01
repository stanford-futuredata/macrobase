package edu.stanford.futuredata.macrobase.sql;

import java.util.Objects;

public class IntPair {

    public int a;
    public int b;

    public IntPair(int a, int b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof IntPair)) {
            return false;
        }

        IntPair other = (IntPair) obj;
        return a == other.a && b == other.b;
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", a, b);
    }
}

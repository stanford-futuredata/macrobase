package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.io.Serializable;
import java.util.Set;

public interface IntSet extends Serializable {
    int getFirst();
    int getSecond();
    int getThird();
    boolean contains(int query);
    Set<Integer> getSet();
}

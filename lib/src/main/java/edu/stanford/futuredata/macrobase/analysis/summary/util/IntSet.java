package edu.stanford.futuredata.macrobase.analysis.summary.util;

import java.util.Set;

public interface IntSet {
    int getFirst();
    int getSecond();
    int getThird();
    boolean contains(int query);
    Set<Integer> getSet();
}

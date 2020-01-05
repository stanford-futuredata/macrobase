package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.List;

public interface Explanation {
    String prettyPrint();
    double numTotal();
    DataFrame toDataFrame(final List<String> attrsToInclude);
}
